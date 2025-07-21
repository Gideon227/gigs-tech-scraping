from pydantic import BaseModel
from typing import Optional


class JobData(BaseModel):
    jobId: str = ""
    title: str = ""
    description: str = ""
    location: str = ""
    country: str = ""
    state: str = ""
    city: str = ""
    jobType: str = ""
    salary: str = ""
    skills: list[str] = []
    experienceLevel: str = ""
    currency: str = ""
    applicationUrl: str = ""
    benefits: list[str] = []
    approvalStatus: str = ""
    brokenLink: bool = False
    jobStatus: str = ""
    responsibilities: list[str] = []
    workSettings: str = ""
    roleCategory: str = ""
    qualifications: list[str] = []
    companyLogo: str = ""
    companyName: str = ""
    ipBlocked: bool = False
    minSalary: float = 0.0
    maxSalary: float = 0.0
    postedDate: Optional[str] = ""
    category:str=""