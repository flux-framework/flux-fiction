import logging 
logger = logging.getLogger(__name__)

def get_loaded_modules(flux_handle):
    """
    Retrieve the list of loaded modules in the current Flux instance.
    """
    try:
        modules = flux_handle.rpc("module.list").get()["mods"]
        return modules
    except Exception as e:
        raise RuntimeError(f"Error retrieving loaded modules: {e}")


def load_missing_modules(flux_handle):
    # TODO: check that necessary modules are loaded
    # if not, load them
    # return an updated list of loaded modules
    # Should be checking for the jobtap module
    loaded_modules = get_loaded_modules(flux_handle)
    pass


def reload_modules(flux_handle, queue_policy = "fcfs", match_policy="first"):
    '''
    To make the resource.R that we submitted to KVS earlier register with the 
    Flux instance, we need to reload both the resource module and scheduler in 
    a specific order 

    (Sched Unload -> Res Unload -> Res Load -> Sched Load)

    It has to be in that order or the scheduler becomes confused

    scheduler parameter defines if we want to use fluxion or sched simple. Eventually, we will define policies here when reloading schedulers
    '''
    sched_module = "sched-simple"
    path = None
    resource_module_path = None
    fluxion_qmanager_path = None
    fluxion_resource_path = None
    # Acquire the path to the scheduling module being used
    # Additionally, acquire the path to the resource module
    for module in get_loaded_modules(flux_handle):
        logger.debug(module)
        if "sched-simple" in module["services"]:
            sched_module = module["name"]
            path = module["path"]
        elif "sched-fluxion-qmanager" in module["name"]:
            sched_module = "fluxion"
            fluxion_qmanager_path = module["path"]
        elif "sched-fluxion-resource" in module["name"]:
            fluxion_resource_path = module["path"]
        elif "resource" in module["name"]:
            resource_module_path = module["path"]
        elif "feasibility" in module["name"]:
            feasibility_module_path = module["path"]

    if path:
        logger.debug(f"{path}")
    elif fluxion_qmanager_path:
        logger.debug(f"fluxion_qmanager_path: {fluxion_qmanager_path}")
    logger.debug(
        "Reloading the '{}' and 'resource' module".format(sched_module))
    if  resource_module_path is not None:
        try:
            if sched_module == "sched-simple":
                flux_handle.rpc("module.remove", payload={
                                "name": "sched-simple"}).get()
            else:
                flux_handle.rpc("module.remove", payload={
                                "name": "sched-fluxion-qmanager"}).get()
                flux_handle.rpc("module.remove", payload={
                                "name": "sched-fluxion-feasibility"}).get()
                flux_handle.rpc("module.remove", payload={
                                "name": "sched-fluxion-resource"}).get()
            flux_handle.rpc("module.remove", payload={
                            "name": "resource"}).get()
            
        except Exception as e:
            logger.error(f"Error removing module: {e}")
        
        
        try:
            flux_handle.rpc("module.load",
                            payload={
                                "path": resource_module_path,
                                "args": ["noverify", "monitor-force-up"],
                            }).get()
            if sched_module == "sched-simple":
                flux_handle.rpc("module.load", payload={
                                "path": path, "args": []}).get()
            else:
                flux_handle.rpc("module.load", payload={
                                "path": fluxion_resource_path, "args": [f"match-policy={match_policy}"]}).get()
                # "queue-policy=conservative"
                flux_handle.rpc("module.load", payload={
                                "path": feasibility_module_path, "args": []}).get()
                flux_handle.rpc("module.load", payload={
                                "path": fluxion_qmanager_path, "args": [f"queue-policy={queue_policy}"]}).get()

        except Exception as e:
            logger.error(e)
    else:
        raise RuntimeError(
            "Unable to get scheduler path (is your scheduler module loaded?)")