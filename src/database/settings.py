from loguru import logger
from tortoise import Tortoise


async def initialize_database() -> None:
    try:
        await Tortoise.init(
            db_url="postgres://avnadmin:AVNS_20HgIJ8fzSf5QE10mLi@pg-3ca5e9bc-hcomb-d8fd.a.aivencloud.com:23323/honeycomb",
            modules={
                "models": [
                    "src.database.models.job_requirements",
                ]
            },
        )

        await Tortoise.generate_schemas()

    except Exception as error:
        logger.error(f"Error while initializing database: {error}")
        input("Press any key to exit")
        exit(0)
