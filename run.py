import asyncio
import yaml

from loguru import logger
from src.main import DiceParser


async def run() -> None:
    with open("settings.yaml", "r") as f:
        config = yaml.safe_load(f)


    if not config.get("threads") or not config.get("search_query"):
        logger.error("Please specify the number of threads and search query in settings.yaml")
        return


    if not isinstance(config.get("threads"), int) or config.get("threads") < 1:
        logger.error("The number of threads must be an integer and greater than 0")
        return

    if not config.get("database_url") or not config.get("database_key"):
        logger.error("Please specify the database_url and database_key in settings.yaml")
        return


    parser = DiceParser(config)
    await parser.start()



if __name__ == "__main__":
    logger.add("logs/dice_parser.log", rotation="1 day", retention="7 days", compression="zip")
    asyncio.run(run())
