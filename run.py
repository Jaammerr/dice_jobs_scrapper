import asyncio
import yaml

from loguru import logger
from src.main import DiceParser
from src.database import initialize_database


async def run() -> None:
    with open("settings.yaml", "r") as f:
        config = yaml.safe_load(f)


    if not config.get("threads") or not config.get("search_query"):
        logger.error("Please specify the number of threads and search query in settings.yaml")
        return


    if not isinstance(config.get("threads"), int) or config.get("threads") < 1:
        logger.error("The number of threads must be an integer and greater than 0")
        return

    await initialize_database()
    parser = DiceParser(config)
    await parser.start()



if __name__ == "__main__":
    logger.add("logs/dice_parser.log", rotation="1 day", retention="7 days", compression="zip")
    asyncio.run(run())
