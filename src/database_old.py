import numpy as np

from datetime import datetime
from threading import Thread
from typing import Any
from postgrest import APIError
from supabase import create_client, Client
from loguru import logger



class Database:
    def __init__(self, config: dict):
        self.supabase: Client = create_client(config["database_url"], config["database_key"])
        self.table = self.supabase.table("jobRequirements")
        self.threads = config["threads"]


    def insert_jobs(self, jobs_data: list, thread_id: int) -> None:

        for job in jobs_data:
            try:
                self.table.insert(job).execute()
                logger.success(f"Thread: {thread_id} | Inserted job to database: {job['jobURL']}")

            except APIError as error:
                error = error.details if error.details else error.message
                logger.error(f"Thread: {thread_id} | Failed to insert job to db: {job['jobURL']} | {error}")


    def insert_jobs_using_threads(self, jobs_data: list) -> None:
        logger.info("Inserting jobs to database..")
        time_now = datetime.now()
        split_jobs: list[np.ndarray[Any, np.dtype]] = np.array_split(jobs_data, self.threads)

        tasks = []
        for number, jobs in enumerate(split_jobs):
            tasks.append(Thread(target=self.insert_jobs, args=(jobs.tolist(), number + 1)))

        for task in tasks:
            task.start()

        for task in tasks:
            task.join()

        total_execution_time = (datetime.now() - time_now).total_seconds()
        logger.success(f"Database execution time: {total_execution_time} seconds")


