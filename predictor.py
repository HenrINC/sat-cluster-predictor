#!/usr/bin/env python3
"""
Satellite Pass Predictor & Job Creator

This runs as a Kubernetes CronJob to:
1. Calculate satellite passes using Skyfield orbital mechanics
2. Create Kubernetes Jobs for each predicted pass
3. Jobs sleep until pass time, then execute recording

Replaces complex Prefect workflows with simple Kubernetes-native approach.
"""

import os
import json
import yaml
import logging
import sys
from datetime import datetime, timedelta, timezone
from typing import List, Dict, Any

import requests
from kubernetes import client, config
from kubernetes.client.rest import ApiException

# Orbital prediction libraries
from skyfield.api import Topos, load, EarthSatellite

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Configuration
CONFIG_PATH = os.getenv('CONFIG_PATH', '/config/config.yml')

def load_config() -> Dict[str, Any]:
    """Load configuration from YAML file"""
    try:
        with open(CONFIG_PATH, 'r') as f:
            config = yaml.safe_load(f)
        logger.info(f"Loaded configuration from {CONFIG_PATH}")
        return config
    except Exception as e:
        logger.error(f"Failed to load config from {CONFIG_PATH}: {e}")
        sys.exit(1)

def setup_kubernetes():
    """Setup Kubernetes client"""
    try:
        # Try in-cluster config first
        config.load_incluster_config()
        logger.info("Using in-cluster Kubernetes configuration")
    except Exception:
        try:
            # Fallback to local config
            config.load_kube_config()
            logger.info("Using local Kubernetes configuration")
        except Exception as e:
            logger.error(f"Failed to load Kubernetes config: {e}")
            sys.exit(1)
    
    return client.BatchV1Api()

def fetch_tle_data() -> Dict[str, List[str]]:
    """Fetch TLE data from Celestrak"""
    logger.info("Fetching TLE data from Celestrak...")
    
    tle_data = {}
    try:
        urls = [
            'https://celestrak.org/NORAD/elements/gp.php?GROUP=weather&FORMAT=tle',
            'https://celestrak.org/NORAD/elements/gp.php?GROUP=noaa&FORMAT=tle'
        ]
        
        for url in urls:
            try:
                response = requests.get(url, timeout=30)
                response.raise_for_status()
                
                lines = response.text.strip().split('\n')
                for i in range(0, len(lines), 3):
                    if i + 2 < len(lines):
                        name = lines[i].strip()
                        line1 = lines[i + 1].strip()
                        line2 = lines[i + 2].strip()
                        
                        if line1.startswith('1 ') and line2.startswith('2 '):
                            tle_data[name] = [name, line1, line2]
                
            except Exception as e:
                logger.warning(f"Failed to fetch from {url}: {e}")
                continue
        
        logger.info(f"Fetched TLE data for {len(tle_data)} satellites")
        
        # Cache TLE data
        os.makedirs('/data', exist_ok=True)
        with open('/data/current_tle.json', 'w') as f:
            json.dump(tle_data, f, indent=2)
        
        return tle_data
        
    except Exception as e:
        logger.error(f"Failed to fetch TLE data: {e}")
        
        # Try cached data
        try:
            if os.path.exists('/data/current_tle.json'):
                with open('/data/current_tle.json', 'r') as f:
                    tle_data = json.load(f)
                logger.info(f"Loaded cached TLE data for {len(tle_data)} satellites")
                return tle_data
        except Exception as cache_e:
            logger.error(f"Failed to load cached TLE data: {cache_e}")
        
        return {}

def predict_passes(tle_data: Dict[str, List[str]], config: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Predict satellite passes using Skyfield for all configured ground stations"""
    prediction_days = config.get('prediction', {}).get('prediction_days', 3)
    logger.info(f"Predicting passes using Skyfield for next {prediction_days} days...")
    
    # Build satellite mapping from config
    satellites = {
        sat['name']: sat 
        for sat in config.get('satellites', [])
    }
    
    all_passes = []
    
    try:
        ts = load.timescale()
        
        now = datetime.utcnow().replace(tzinfo=timezone.utc)
        end_time = now + timedelta(days=prediction_days)
        
        t0 = ts.utc(now.year, now.month, now.day, now.hour, now.minute, now.second)
        t1 = ts.utc(end_time.year, end_time.month, end_time.day, end_time.hour, end_time.minute, end_time.second)
        
        # Process each ground station
        for ground_station in config.get('ground_stations', []):
            station_name = ground_station.get('name', 'Unknown')
            station_lat = ground_station.get('latitude', 45.0)
            station_lon = ground_station.get('longitude', 2.0)
            station_alt = ground_station.get('altitude', 100)
            min_elevation = ground_station.get('minimum_elevation', 10.0)
            station_satellites = ground_station.get('satellites', [])
            
            logger.info(f"Processing ground station: {station_name}")
            
            station = Topos(
                latitude_degrees=station_lat,
                longitude_degrees=station_lon,
                elevation_m=station_alt
            )
            
            # Process each satellite for this ground station
            for sat_name, sat_config in satellites.items():
                norad_id = sat_config['id']
                frequency = sat_config.get('frequency', 137.5)
                
                if norad_id not in station_satellites:
                    continue
                    
                if sat_name not in tle_data:
                    logger.warning(f"No TLE data for {sat_name}")
                    continue
                    
                try:
                    tle_lines = tle_data[sat_name]
                    satellite = EarthSatellite(tle_lines[1], tle_lines[2], sat_name, ts)
                    
                    t, events = satellite.find_events(station, t0, t1, altitude_degrees=min_elevation)
                    
                    for ti, event in zip(t, events):
                        if event == 0:  # Rise event
                            rise_time = ti.utc_datetime()
                            
                            # Find culmination and set
                            max_time = None
                            set_time = None
                            max_elevation = 0
                            
                            for tj, eventj in zip(t, events):
                                if tj.utc_datetime() > rise_time:
                                    if eventj == 1 and max_time is None:  # Culmination
                                        max_time = tj.utc_datetime()
                                        difference = satellite - station
                                        topocentric = difference.at(tj)
                                        alt, az, distance = topocentric.altaz()
                                        max_elevation = alt.degrees
                                    elif eventj == 2 and set_time is None:  # Set
                                        set_time = tj.utc_datetime()
                                        break
                            
                            if max_time and set_time and max_elevation > min_elevation:
                                duration = int((set_time - rise_time).total_seconds())
                                
                                pass_info = {
                                    'satellite': sat_name,
                                    'norad_id': norad_id,
                                    'frequency': frequency,
                                    'start_time': rise_time.isoformat(),
                                    'max_time': max_time.isoformat(),
                                    'end_time': set_time.isoformat(),
                                    'duration': duration,
                                    'max_elevation': round(max_elevation, 2),
                                    'ground_station': {
                                        'name': station_name,
                                        'latitude': station_lat,
                                        'longitude': station_lon,
                                        'altitude': station_alt
                                    }
                                }
                                all_passes.append(pass_info)
                                
                except Exception as e:
                    logger.warning(f"Failed to predict passes for {sat_name} at {station_name}: {e}")
                    continue
        
        all_passes.sort(key=lambda x: x['start_time'])
        logger.info(f"Predicted {len(all_passes)} passes across all ground stations")
        return all_passes
        
    except Exception as e:
        logger.error(f"Failed to predict passes with Skyfield: {e}")
        return []

def create_recording_job(k8s_client, pass_info: Dict[str, Any], job_number: int) -> bool:
    """Create a Kubernetes Job for recording a satellite pass"""
    try:
        sat_name = pass_info['satellite']
        start_time = datetime.fromisoformat(pass_info['start_time'].replace('Z', '+00:00'))
        now = datetime.utcnow().replace(tzinfo=timezone.utc)
        
        # Calculate sleep time until pass starts
        sleep_seconds = max(0, int((start_time - now).total_seconds()))
        
        # Create job name with timestamp
        timestamp = start_time.strftime('%m%d-%H%M')
        job_name = f"record-{sat_name.lower().replace(' ', '-')}-{timestamp}-{job_number:03d}"
        
        # Environment variables for the recorder
        env_vars = [
            client.V1EnvVar(name="SATELLITE_NAME", value=sat_name),
            client.V1EnvVar(name="NORAD_ID", value=str(pass_info['norad_id'])),
            client.V1EnvVar(name="FREQUENCY", value=str(pass_info['frequency'])),
            client.V1EnvVar(name="START_TIME", value=pass_info['start_time']),
            client.V1EnvVar(name="END_TIME", value=pass_info['end_time']),
            client.V1EnvVar(name="DURATION", value=str(pass_info['duration'])),
            client.V1EnvVar(name="MAX_ELEVATION", value=str(pass_info['max_elevation'])),
            client.V1EnvVar(name="SLEEP_SECONDS", value=str(sleep_seconds)),
            client.V1EnvVar(name="GROUND_STATION_LAT", value=str(pass_info['ground_station']['latitude'])),
            client.V1EnvVar(name="GROUND_STATION_LON", value=str(pass_info['ground_station']['longitude'])),
            client.V1EnvVar(name="GROUND_STATION_ALT", value=str(pass_info['ground_station']['altitude']))
        ]
        
        # Container spec
        container = client.V1Container(
            name="recorder",
            image="henriinc/recorder:latest",
            env=env_vars,
            volume_mounts=[
                client.V1VolumeMount(
                    name="recordings-storage",
                    mount_path="/recordings"
                )
            ],
            resources=client.V1ResourceRequirements(
                requests={"memory": "128Mi", "cpu": "100m"},
                limits={"memory": "256Mi", "cpu": "200m"}
            )
        )
        
        # Pod spec
        pod_spec = client.V1PodSpec(
            containers=[container],
            restart_policy="Never",
            volumes=[
                client.V1Volume(
                    name="recordings-storage",
                    persistent_volume_claim=client.V1PersistentVolumeClaimVolumeSource(
                        claim_name="recordings-pvc"
                    )
                )
            ]
        )
        
        # Job spec
        job_spec = client.V1JobSpec(
            template=client.V1PodTemplateSpec(
                metadata=client.V1ObjectMeta(
                    labels={
                        "app": "satellite-recorder",
                        "satellite": sat_name.lower().replace(' ', '-'),
                        "pass-date": start_time.strftime('%Y-%m-%d')
                    }
                ),
                spec=pod_spec
            ),
            backoff_limit=1,
            ttl_seconds_after_finished=3600  # Clean up after 1 hour
        )
        
        # Job object
        job = client.V1Job(
            api_version="batch/v1",
            kind="Job",
            metadata=client.V1ObjectMeta(
                name=job_name,
                namespace="recordings",
                labels={
                    "app": "satellite-recorder",
                    "satellite": sat_name.lower().replace(' ', '-'),
                    "managed-by": "predictor"
                }
            ),
            spec=job_spec
        )
        
        # Create the job
        k8s_client.create_namespaced_job(namespace="recordings", body=job)
        
        logger.info(f"✅ Created job {job_name} for {sat_name} (sleeps {sleep_seconds}s)")
        return True
        
    except ApiException as e:
        logger.error(f"Failed to create job for {pass_info['satellite']}: {e}")
        return False
    except Exception as e:
        logger.error(f"Unexpected error creating job for {pass_info['satellite']}: {e}")
        return False

def main():
    """Main predictor function - creates recording jobs for all passes"""
    logger.info("=== Satellite Pass Predictor & Job Creator ===")
    logger.info(f"Configuration: {CONFIG_PATH}")
    
    try:
        # Load configuration
        config = load_config()
        logger.info(f"Loaded config with {len(config.get('ground_stations', []))} ground stations")
        logger.info(f"Tracking {len(config.get('satellites', []))} satellites")
        
        # Setup Kubernetes client
        k8s_client = setup_kubernetes()
        
        # Fetch TLE data
        logger.info("Fetching TLE data...")
        tle_data = fetch_tle_data()
        if not tle_data:
            logger.error("No TLE data available")
            sys.exit(1)
        
        # Predict passes
        logger.info("Calculating satellite passes...")
        passes = predict_passes(tle_data, config)
        
        if not passes:
            logger.warning("No passes predicted")
            return
        
        # Create recording jobs for each pass
        logger.info(f"Creating {len(passes)} recording jobs...")
        successful_jobs = 0
        
        for i, pass_info in enumerate(passes, 1):
            if create_recording_job(k8s_client, pass_info, i):
                successful_jobs += 1
        
        logger.info(f"✅ Job creation complete: {successful_jobs}/{len(passes)} jobs created")
        
        # Summary
        summary = {
            "timestamp": datetime.utcnow().isoformat() + "Z",
            "prediction_days": config.get('prediction', {}).get('prediction_days', 3),
            "passes_predicted": len(passes),
            "jobs_created": successful_jobs,
            "ground_stations": len(config.get('ground_stations', [])),
            "satellites": len(config.get('satellites', [])),
            "method": "skyfield + kubernetes"
        }
        
        logger.info(f"Summary: {summary}")
        
    except Exception as e:
        logger.error(f"Prediction job failed: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
