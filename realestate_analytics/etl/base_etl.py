from typing import Dict, List, Union
from abc import ABC, abstractmethod
from pathlib import Path
from datetime import datetime, timedelta
import pytz


from ..data.caching import FileBasedCache
from ..data.es import Datastore
from ..data.bq import BigQueryDatastore
from ..data.archive import Archiver

import logging

class BaseETLProcessor(ABC):
  def __init__(self, job_id: str, 
               datastore: Datastore,                
               bq_datastore: BigQueryDatastore = None,
               cache_dir: Union[str, Path] = None,
               archive_dir: Union[str, Path] = None):
    self.logger = logging.getLogger(self.__class__.__name__)

    self.job_id = job_id
    self.datastore = datastore
    self.bq_datastore = bq_datastore
    if not self.check_dependencies():
      raise Exception("Failed to establish connection to required datastores")

    self.cache_dir = Path(cache_dir) if cache_dir else None
    if self.cache_dir:
      self.cache_dir.mkdir(parents=True, exist_ok=True)

    self.cache = FileBasedCache(cache_dir=self.cache_dir)
    self.logger.info(f'cache dir: {self.cache.cache_dir}')

    self.last_run_key = f"{self.__class__.__name__}_last_run"
    last_run = self.cache.get(self.last_run_key)
    self.logger.info(f"Last run: {last_run}")
    
    self.archiver = Archiver(archive_dir or self.cache.cache_dir / 'archives')

    self.stages = ['extract', 'transform', 'load']
    self.extra_stages = []
    self.extra_cleanup_stages = []
    
    self.use_utc = True

  def load_config(self, config_path: Union[str, Path]):
    # Implement configuration loading logic
    pass

  def run(self):
    if self._was_success('all'):
      self.logger.info(f"Job {self.job_id} has already run successfully.")
      return
    
    def _cleanup():
      for stage in self.stages + self.extra_stages + self.extra_cleanup_stages:
        self._unmark_success(stage)
      self.delete_checkpoints_data()

    def add_extra_stage(stage_name: str):
      self.extra_stages.append(stage_name)

    def add_extra_cleanup_stage(stage_name: str):
      self.extra_cleanup_stages.append(stage_name)

    # Allow subclasses to define extra stages before execution
    self.setup_extra_stages(add_extra_stage, add_extra_cleanup_stage)

    for stage in self.stages:
      method = getattr(self, f'_{stage}')
      method()

    for stage in self.extra_stages:
      method = getattr(self, stage)
      method()

    all_stages = self.stages + self.extra_stages
    if all(self._was_success(stage) for stage in all_stages):
      _cleanup()
      self._mark_success('all')
      self.logger.info(f"Job {self.job_id} completed successfully.")

    # self._extract()
    # self._transform()
    # self._load()

    # if the whole job is successful, clean up the temporary checkpoints
    # stages = ['extract', 'transform', 'load']
    # if all(self._was_success(stage) for stage in stages):
    #   for stage in stages:
    #     self._unmark_success(stage)

    #   # NOTE: subclass should also remove their own intermediate checkpoints cached
    #   self.delete_checkpoints_data()

    #   self._mark_success('all')
    #   self.logger.info(f"Job {self.job_id} completed successfully.")

  def setup_extra_stages(self, add_extra_stage, add_extra_cleanup_stage):
    # This method can be overridden by subclasses to add extra stages
    # e.g. as done in LastMthMetricsProcessor
    # add_extra_stage('end_of_mth_run')
    # add_extra_cleanup_stage('compute_last_month_metrics')
    # add_extra_cleanup_stage('remove_deleted_listings')
    # add_extra_cleanup_stage('update_mkt_trends')
    pass

  def full_refresh(self):
    """
    Perform a full refresh of the data, resetting to initial state and re-extracting all data.
    """
    self.logger.info("Starting full refresh...")
    self.cleanup()
    self._extract()
    self._transform()
    self._load()
    self.logger.info("Full refresh completed.")

  def _extract(self, from_cache=False):
    self.logger.info("Extract")
    self.pre_extract()
    try:
      self.extract(from_cache=from_cache)  # Implementation to be provided by subclasses      
    finally:
      self.post_extract()
      
  def _transform(self):
    self.logger.info("Transform")
    self.pre_transform()
    try:
      self.transform()  # Implementation to be provided by subclasses
    finally:
      self.post_transform()

  def _load(self):
    self.logger.info("Load")
    self.pre_load()
    try:
      success, failed = self.load()    # Implementation to be provided by subclasses
      total_attempts = success + len(failed)
      if total_attempts == 0:
        self.logger.info("No documents were attempted to be updated.")       
      return success, failed
    except Exception as e:
      self.logger.error(f"Error in load: {e}", exc_info=True)
      return 0, []
    finally:
      self.post_load()


  def get_current_datetime(self):
    if self.use_utc:
      return datetime.now(pytz.utc).replace(tzinfo=None)
    else:
      return datetime.now()
    

  def check_dependencies(self) -> bool:
    if not self.datastore.ping():
      self.logger.error(f"Cannot connect to ES at {self.datastore.es_host}:{self.datastore.es_port}")
      return False
    if self.bq_datastore and not self.bq_datastore.ping():
      self.logger.error(f"Cannot connect to BigQuery at {self.bq_datastore.project_id}")
      return False
    return True
  
  def close(self):
    if hasattr(self, 'datastore') and self.datastore:
      self.datastore.close()
    if hasattr(self, 'bq_datastore') and self.bq_datastore:
      self.bq_datastore.close()
    
  

  def _load_from_cache(self):
    """
    Base method to load data from cache.
    Subclasses should override this method with their specific implementation.
    """
    if not self.cache.cache_dir:
      self.logger.error("Cache directory not set. Cannot load from cache.")
      raise ValueError("Cache directory not set. Cannot load from cache.")

  def _save_to_cache(self):
    """
    Base method to save data to cache.
    Subclasses should override this method with their specific implementation.
    """
    if not self.cache.cache_dir:
      self.logger.error("Cache directory not set. Cannot save to cache.")
      raise ValueError("Cache directory not set. Cannot save to cache.")

  def _extract_from_datastore(self):
    """
    Optional method to extract data from datastore.
    Subclasses can override this method if needed.
    """
    self.logger.debug("_extract_from_datastore not implemented in subclass")


  def cleanup(self):
    """Basic cleanup method that can be extended by subclasses."""
    if not self.cache.cache_dir:
      self.logger.error("Cache directory not set. Cannot cleanup cache.")      
      raise ValueError("Cache directory not set. Cannot cleanup cache.")
    
    self.cache.delete(self.last_run_key)
    # Subclasses should call super().cleanup() and add their specific cleanup logic

  def reset(self):
    self.cleanup()

  def delete_checkpoints_data(self):
    # Subclasses should override this method to delete their own intermediate checkpoints
    pass


  # checkpoints marker helper methods
  def _was_success(self, stage: str) -> bool:
    return self.cache.get(f"{self.job_id}_{stage}_success") is not None

  def _mark_success(self, stage: str):
    self.cache.set(f"{self.job_id}_{stage}_success", " ")

  def _unmark_success(self, stage: str):
    self.cache.delete(f"{self.job_id}_{stage}_success")


  # Event hooks
  def pre_extract(self):
    if getattr(self, 'simulate_failure_at', None) == 'extract':
      raise Exception("Simulated failure in extract")
    
  def pre_transform(self):
    if getattr(self, 'simulate_failure_at', None) == 'transform':
      raise Exception("Simulated failure in transform")
    
  def pre_load(self):
    if getattr(self, 'simulate_failure_at', None) == 'load':
      raise Exception("Simulated failure in load")
    
  def post_extract(self):
    if getattr(self, 'simulate_success_at', None) == 'extract':
      self._mark_success('extract')

  def post_transform(self):
    if getattr(self, 'simulate_success_at', None) == 'transform':
      self._mark_success('transform')

  def post_load(self):
    if getattr(self, 'simulate_success_at', None) == 'load':
      self.logger.info("Simulating success in load")
      self._mark_success('load')


  def extract(self, from_cache=False):
    if from_cache:
      self._load_from_cache()
    else:
      self._extract_from_datastore()


  @abstractmethod
  def transform(self):
    raise NotImplementedError("subclass must implement this method")
  
  @abstractmethod
  def load(self):
    raise NotImplementedError("subclass must implement this method")


if __name__ == "__main__":
  job_id = 'test_job_0'
  uat_datastore = Datastore(host='localhost', port=9201)
  prod_datastore = Datastore(host='localhost', port=9202)
  bq_datastore = BigQueryDatastore()

  processor = BaseETLProcessor(job_id, uat_datastore, bq_datastore)

  processor.run()