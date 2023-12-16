from dataclasses import dataclass


@dataclass
class JobOffer:
    guid: str
    title: str
    payrates: list[str]
    locations: list[str]
    posted_date: str
    company_name: str
    job_url: str
    employmentType: str
    travel_requirements: list[str] = None
    job_description: str = None
    easy_apply: bool = False
    contract_types: list[str] = None
    skills: list[str] = None
