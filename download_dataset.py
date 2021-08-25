import os
import sys
import multiprocessing
from   joblib                                                            import Parallel, delayed
from   allensdk.brain_observatory.ecephys.ecephys_project_api.utilities  import build_and_execute
from   allensdk.brain_observatory.ecephys.ecephys_project_api.rma_engine import RmaEngine
from   allensdk.brain_observatory.ecephys.ecephys_project_cache          import EcephysProjectCache

data_directory = 'data/ecephys_cache_dir' # remember to change this to something that exists on your machine
manifest_path  = os.path.join(data_directory, "manifest.json")
rma_engine     = RmaEngine(scheme="http", host="api.brain-map.org")
cache          = EcephysProjectCache.from_warehouse(manifest=manifest_path)
sessions       = cache.get_session_table()

def retrieve_link(session_id):
    
    well_known_files = build_and_execute(
        (
        "criteria=model::WellKnownFile"
        ",rma::criteria,well_known_file_type[name$eq'EcephysNwb']"
        "[attachable_type$eq'EcephysSession']"
         r"[attachable_id$eq{{session_id}}]"
    ),
    engine=rma_engine.get_rma_tabular, 
    session_id=session_id
    )
            
    return 'http://api.brain-map.org/' + well_known_files['download_link'].iloc[0]

def download_session(session_id):
    # Get link to download data
    link = retrieve_link(session_id)
    # Create folder to store data
    os.system("mkdir " + os.path.join(data_directory, f"session_{session_id}"))
    # Download data
    os.system("wget "+ link)
    # Transfer data to proper directory
    os.system("mv " + link.split("http://api.brain-map.org//api/v2/well_known_file_download/",1)[1] + " " + os.path.join(data_directory, f"session_{session_id}",f"ecephys_session_{session_id}.nwb"))

n_jobs = int(sys.argv[-1])
Parallel(n_jobs=n_jobs, backend='loky', timeout=1e6)(delayed(download_session)(sid) for sid in sessions.index.values)

