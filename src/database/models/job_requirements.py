import asyncio
from datetime import datetime

from loguru import logger
from tortoise import fields, Model



class Jobs(Model):
    title = fields.TextField(null=True)
    contractType = fields.TextField(null=True)
    location = fields.TextField(null=True)
    payrate = fields.TextField(null=True)
    postedDate = fields.DatetimeField(null=True)
    companyName = fields.TextField(null=True)
    jobURL = fields.TextField(null=True)
    jobDescription = fields.TextField(null=True)
    easyApply = fields.BooleanField(null=True)
    travelRequirement = fields.TextField(null=True)
    skills = fields.TextField(null=True)
    guid = fields.UUIDField(null=True)
    created_at = fields.DatetimeField(auto_now_add=True)

    class Meta:
        table = "jobRequirements"


    @classmethod
    async def add_job(cls, job_data: dict) -> bool:
        try:
            await cls.create(
                title=job_data["title"],
                contractType=job_data["contractType"],
                location=job_data["location"],
                payrate=job_data["payrate"],
                postedDate=job_data["postedDate"],
                companyName=job_data["companyName"],
                jobURL=job_data["jobURL"],
                jobDescription=job_data["jobDescription"],
                easyApply=job_data["easyApply"],
                travelRequirement=job_data["travelRequirement"],
                skills=job_data["skills"],
                guid=job_data["guid"],
            )
            return True

        except Exception as error:
            logger.error(f"Error while adding job to database: {error}")
            return False





    @classmethod
    async def add_multi_jobs(cls, jobs: list) -> None:
        logger.info("Inserting jobs to database..")
        time_now = datetime.now()

        tasks = [
            asyncio.create_task(cls.add_job(job))
            for job in jobs
        ]
        await asyncio.gather(*tasks)

        total_execution_time = (datetime.now() - time_now).total_seconds()
        logger.success(f"Database execution time: {total_execution_time} seconds")
