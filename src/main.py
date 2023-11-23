import asyncio
from pathlib import Path

import aiofiles
import httpx
import pyuseragents
import numpy as np

from loguru import logger
from bs4 import BeautifulSoup
from datetime import datetime
from typing import Any
from aiocsv import AsyncWriter
from src.models import JobOffer


class DiceParser(httpx.AsyncClient):
    def __init__(self, config: dict):
        super().__init__()
        self.API_URL = 'https://job-search-api.svc.dhigroupinc.com/v1/dice'
        self.user_agent = pyuseragents.random()
        self.config = config
        self.parser_status = False

        self.headers = {
            'authority': 'job-search-api.svc.dhigroupinc.com',
            'accept': 'application/json, text/plain, */*',
            'accept-language': 'en-US,en;q=0.9,ru;q=0.8',
            'origin': 'https://www.dice.com',
            'referer': 'https://www.dice.com/',
            'user-agent': self.user_agent,
            'x-api-key': '1YAt0R9wBg4WfsF9VB2778F5CHLAPMVW3WAZcKd8',
        }

        self.search_params = {
            'q': self.config.get('search_query'),
            'countryCode2': 'US',
            'radius': '30',
            'radiusUnit': 'mi',
            'page': '1',
            'pageSize': '20',
            'facets': 'employmentType|postedDate|workFromHomeAvailability|employerType|easyApply|isRemote',
            'fields': 'id|jobId|guid|summary|title|postedDate|modifiedDate|jobLocation.displayName|detailsPageUrl|salary|clientBrandId|companyPageUrl|companyLogoUrl|positionId|companyName|employmentType|isHighlighted|score|easyApply|employerType|workFromHomeAvailability|isRemote|debug',
            'culture': 'en',
            'recommendations': 'true',
            'interactionId': '0',
            'fj': 'true',
            'includeRemote': 'true',
        }



    async def get_all_jobs(self) -> list:
        url = f'{self.API_URL}/jobs/search'

        response = await self.get(url, params=self.search_params)
        response.raise_for_status()

        total_count = response.json().get("meta").get("totalResults")
        if not total_count:
            raise Exception('Failed to get total count of jobs')

        logger.info(f'Total count of jobs: {total_count}')
        self.search_params['pageSize'] = total_count

        response = await self.get(url, params=self.search_params)
        response.raise_for_status()

        jobs: list = response.json().get("data")
        if not jobs:
            raise Exception('Failed to get jobs')

        return jobs


    async def get_job_details(self, job_data: dict, thread_id: int) -> JobOffer or None:
        try:
            url = job_data["detailsPageUrl"]
            if "apply-redirect" in url:
                logger.warning(f"Thread: {thread_id} | Job offer is not available | {url}")
                return None

            response = await self.get(url)
            response.raise_for_status()

            soup = BeautifulSoup(response.text, 'html.parser')

            contracts = soup.find('div', class_='job-overview_chipContainer__E4zOO', attrs={'data-cy': 'employmentDetails'})
            if contracts:
                contracts = contracts.find_all('span')
                contracts = [contract.text for contract in contracts]

            else:
                contracts = None


            skills = soup.find('div', class_='Skills_chipContainer__mlLa7', attrs={'data-cy': 'skillsList'})
            if skills:
                skills = skills.find_all('span')
                skills = [skill.text for skill in skills]

            else:
                skills = None

            locations = soup.find('div', class_='job-overview_chipContainer__E4zOO', attrs={'data-cy': 'locationDetails'})
            if locations:
                locations = locations.find_all('span')
                locations = [location.text for location in locations]

            else:
                locations = None

            payrates = soup.find('div', class_='job-overview_chipContainer__E4zOO', attrs={'data-cy': 'payDetails'})
            if payrates:
                payrates = payrates.find_all('span')
                payrates = [payrate.text for payrate in payrates]

            else:
                payrates = None

            job_description = soup.find('div', {'data-testid': 'jobDescriptionHtml'})
            if job_description:
                job_description = job_description.get_text(separator='\n')

            else:
                job_description = None

            travel_requirements = soup.find('div', class_='job-overview_chipContainer__E4zOO', attrs={'data-cy': 'travelDetails'})
            if travel_requirements:
                travel_requirements = travel_requirements.find_all('span')
                travel_requirements = [requirement.text for requirement in travel_requirements]

            else:
                travel_requirements = None

            job_offer = JobOffer(
                travel_requirements=travel_requirements,
                title=job_data["title"],
                locations=locations,
                payrates=payrates,
                posted_date=job_data["postedDate"],
                job_url=job_data["detailsPageUrl"],
                company_name=job_data["companyName"],
                employmentType=job_data["employmentType"],
                easy_apply=job_data.get("easyApply"),
                contract_types=contracts,
                skills=skills,
                job_description=job_description,
            )

            logger.info(f"Thread: {thread_id} | Got job offer for url: {job_offer.job_url}")
            return job_offer

        except Exception as error:
            logger.warning(f"Thread: {thread_id} | Failed to get job details | {error}")
            return None



    async def get_jobs_details(self, jobs_data: list, queue: asyncio.Queue, thread_id: int) -> None:
        logger.info(f"Thread {thread_id} | Got {len(jobs_data)} jobs")

        for job in jobs_data:
            try:
                job_offer = await self.get_job_details(job, thread_id)
                if job_offer:
                    await queue.put(job_offer)

            except Exception as error:
                logger.warning(f"Thread: {thread_id} | Failed to get job details | {error}")



    async def monitor_queue(self, queue: asyncio.Queue) -> None:
        file_path = Path(__file__).parent.parent / "results.csv"
        async with aiofiles.open(file_path, mode="w", encoding="utf-8", newline="") as afp:
            writer = AsyncWriter(afp, dialect="unix")
            await writer.writerow([
                "Title",
                "Location",
                "Payrate",
                "Posted Date",
                "Company Name",
                "Job Url",
                "Job Description",
                "Easy Apply",
                "Travel Requirements",
                "Contract Types",
                "Skills",
            ])

            while self.parser_status:
                job_offer: JobOffer = await queue.get()
                job_offer.job_description = job_offer.job_description.replace('\n', ' ')

                await writer.writerow([
                    job_offer.title,
                    ", ".join(job_offer.locations) if job_offer.locations else None,
                    ", ".join(job_offer.payrates) if job_offer.payrates else None,
                    job_offer.posted_date,
                    job_offer.company_name,
                    job_offer.job_url,
                    # job_offer.employmentType,
                    job_offer.job_description,
                    job_offer.easy_apply,
                    ", ".join(job_offer.travel_requirements) if job_offer.travel_requirements else None,
                    ", ".join(job_offer.contract_types) if job_offer.contract_types else None,
                    ", ".join(job_offer.skills) if job_offer.skills else None,
                ])




    async def start(self):
        logger.success("Parser started..")
        self.parser_status = True

        time_now = datetime.now()
        queue = asyncio.Queue()
        asyncio.run_coroutine_threadsafe(self.monitor_queue(queue), asyncio.get_event_loop())

        try:
            all_jobs: list = await self.get_all_jobs()
            split_jobs: list[np.ndarray[Any, np.dtype]] = np.array_split(all_jobs, self.config.get('threads'))
        except Exception as error:
            logger.error(f"Failed to get jobs | {error} | Parser stopped..")
            return

        tasks = [
            asyncio.create_task(self.get_jobs_details(jobs.tolist(), queue, number + 1))
            for number, jobs in enumerate(split_jobs)
        ]
        await asyncio.gather(*tasks)

        total_execution_time = (datetime.now() - time_now).total_seconds()
        self.parser_status = False
        logger.success(f"Parser finished.. | Execution time: {total_execution_time} seconds")
