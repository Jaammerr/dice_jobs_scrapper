## Generally

The script is designed to gather all job listings on the website https://www.dice.com/ and import them into a CSV format.

The script is entirely asynchronous and utilizes asynchronous threads (tasks).


## Configuration (settings.yaml)
`threads` - number of threads to use for scraping

`search_query` - search query to use for scraping jobs


## Installation
Requirements: Python 3.10+

1. Clone the repository
2. Open the folder in your terminal
3. Create a virtual environment using `python -m venv venv`
4. Activate the virtual environment using `source venv/bin/activate`
5. Install the requirements using `pip install -r requirements.txt`
6. Run the script using `python run.py`


## Output

The script will output a CSV file with name: `results.csv`




