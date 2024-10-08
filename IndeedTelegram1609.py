import requests
from bs4 import BeautifulSoup
import pandas as pd
from datetime import date, datetime
import time
import os
import json
from dotenv import load_dotenv
import random
from selenium import webdriver 
from selenium.webdriver.common.desired_capabilities import DesiredCapabilities

# Load environment variables
load_dotenv()

# Configuration
TELEGRAM_BOT_TOKEN = os.getenv('TELEGRAM_BOT_TOKEN')  # Replace with your bot token securely
TELEGRAM_CHAT_ID = os.getenv('TELEGRAM_CHAT_ID')

# Check if Telegram credentials are loaded
if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
    raise ValueError("Error: Telegram bot token or chat ID is not set in environment variables.")

SENT_ALERTS_FILE = "sent_alerts.csv"

def load_sent_alerts():
    """Load the list of already sent alerts from the CSV file."""
    if os.path.exists(SENT_ALERTS_FILE):
        return pd.read_csv(SENT_ALERTS_FILE)['ID'].tolist()
    else:
        return []

def save_sent_alert(alert_id):
    """Save a new alert ID to the CSV file."""
    df = pd.DataFrame({"ID": [alert_id]})
    file_exists = os.path.exists(SENT_ALERTS_FILE)
    with open(SENT_ALERTS_FILE, mode='a', newline='', encoding='utf-8') as f:
        df.to_csv(f, header=not file_exists, index=False)

# Date and time object
today = date.today()
now = datetime.now().strftime("%H:%M:%S")

# Data frame and series utilities
template = {"Job Title": [], "Company Name": [], "Location": [], "Job Description": [], "Link": []}
errorTemplate = {"Error": [], "Location": [], "Date": [], "Time": []}
errorDataFrame = pd.DataFrame(errorTemplate)
Main_DataFrame = pd.DataFrame(template)

# Initialize variables
titles = []
names = []
locations = []
job_descriptions = []
links = []

def send_telegram_alert(message):
    retries = 3
    for attempt in range(retries):
        try:
            url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
            payload = {
                'chat_id': TELEGRAM_CHAT_ID,
                'text': message,
                'parse_mode': 'HTML'
            }
            response = requests.post(url, data=payload)
            response.raise_for_status()
            print("Telegram alert sent successfully.")
            time.sleep(1)  # Delay to avoid hitting rate limits
            return
        except requests.exceptions.HTTPError as errh:
            if response.status_code == 429:  # Rate limit error
                print(f"Rate limit exceeded. Attempt {attempt + 1}/{retries}.")
                time.sleep(60)  # Wait for a minute before retrying
            else:
                print(f"HTTP Error: {errh}")
                return
        except requests.exceptions.RequestException as err:
            print(f"Something went wrong: {err}")
            return

def getPage(url):
    options = webdriver.ChromeOptions()
    options.add_argument("--headless")
    options.add_argument("start-maximized")
    options.add_argument("disable-infobars")
    options.add_argument("--disable-extensions")
    
    # User-Agent for the requests
    user_agents = [
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/85.0.4183.102 Safari/537.36",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_5) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/13.1.1 Safari/605.1.15",
    ]
    options.add_argument(f"user-agent={random.choice(user_agents)}")

    driver = webdriver.Chrome(options=options)
    try:
        driver.get(url)
        page_content = driver.page_source
        soup = BeautifulSoup(page_content, 'html.parser')
        result = soup.find(id="mosaic-provider-jobcards")

        if result is None:
            raise ValueError("Job cards container not found in the page")

        return soup
    except Exception as ex:
        errorLog_file(str(ex), "Soup", today, now)
        return None
    finally:
        driver.quit()

def jobCard(url):
    try:
        soup = getPage(url)
        if soup is None:
            return

        title_elements = soup.find_all("h2", class_="jobTitle css-198pbd eu4oa1w0")
        company_elements = soup.find_all("span", class_="css-63koeb eu4oa1w0")
        location_elements = soup.find_all("div", class_="css-1p0sjhy eu4oa1w0")
        description_elements = soup.find_all("div", class_="css-9446fg eu4oa1w0")
        job_link_elements = soup.find_all("a", class_="jcs-JobTitle css-jspxzf eu4oa1w0")

        # Handle cases where no data is found
        if not any([title_elements, company_elements, location_elements, description_elements, job_link_elements]):
            print(f"Some elements are missing in the page content for URL: {url}")
            return

        titles.extend([title.text.strip() for title in title_elements])
        names.extend([name.text.strip() for name in company_elements])
        locations.extend([location.text.strip() for location in location_elements])
        job_descriptions.extend([" ".join([li.text.strip() for li in desc.find_all("li")]) if desc else "N/A" for desc in description_elements])
        links.extend([f"https://www.indeed.com{link.get('href')}" if link else "N/A" for link in job_link_elements])

        print("Success: Job card data fetched")
        return (titles, names, locations, job_descriptions, links)

    except Exception as ex:
        errorLog_file(str(ex), "jobCard Function Failed", today, now)
        print("An error occurred in jobCard: ", ex)

def createDataFrame():
    try:
        # Ensure all lists have the same length by filtering incomplete data
        length = len(titles)
        job_card_data = []

        for i in range(length):
            if i < len(names) and i < len(locations) and i < len(job_descriptions) and i < len(links):
                job_card_data.append({
                    "Job Title": titles[i],
                    "Company Name": names[i],
                    "Location": locations[i],
                    "Job Description": job_descriptions[i],
                    "Link": links[i]
                })
            else:
                print(f"Incomplete data at index {i}, skipping.")

        # Create DataFrame if we have valid data
        if job_card_data:
            print("Data Frame successfully created")
            return job_card_data
        else:
            raise ValueError("No valid data to create DataFrame")

    except Exception as ex:
        errorLog_file(str(ex), "createDataFrame", today, now)
        print("An error occurred in createDataFrame: ", ex)
        return None

def errorLog_file(error, loc, date, time):
    try:
        error_data = {
            "Error": error,
            "Location": loc,
            "Date": str(date),
            "Time": time
        }

        if os.path.exists("error.json"):
            with open("error.json", 'r+', encoding='utf-8') as f:
                data = json.load(f)
                data.append(error_data)
                f.seek(0)
                json.dump(data, f, ensure_ascii=False, indent=4)
        else:
            with open("error.json", 'w', encoding='utf-8') as f:
                json.dump([error_data], f, ensure_ascii=False, indent=4)

    except Exception as ex:
        print("An error occurred while logging error: ", ex)

def DriverMain(listOfposition):
    global titles, names, locations, job_descriptions, links
    titles.clear()
    names.clear()
    locations.clear()
    job_descriptions.clear()
    links.clear()

    sent_alerts = load_sent_alerts()  # Load already sent alerts

    url = None  # Initialize url variable

    try:
        for job_title in listOfposition:
            url = f"https://in.indeed.com/jobs?q={job_title}&l=India&from=searchOnDesktopSerp"
            jobCard(url)

            if not titles or not names or not locations or not job_descriptions or not links:
                print(f"No data retrieved for {job_title}. Skipping this job title.")
                continue  # Skip to the next job title

            for i in range(len(titles)):
                try:
                    job_id = links[i].split("jk=")[-1].split("&")[0]

                    # Check if the job ID has already been alerted
                    if job_id not in sent_alerts:
                        message = (f"<b>Job Title:</b> {titles[i]}\n"
                                   f"<b>Company Name:</b> {names[i]}\n"
                                   f"<b>Location:</b> {locations[i]}\n"
                                   f"<b>Description:</b> {job_descriptions[i][:300]}...\n"
                                   f"<b>To Apply:</b> <a href='{links[i]}'>Click Here</a>\n\n"
                                   f"<a href='t.me/PharmakonSpace'>More_Job</a>")

                        send_telegram_alert(message)
                        save_sent_alert(job_id)  # Mark this job as alerted
                        sent_alerts.append(job_id)

                except Exception as ex:
                    errorLog_file(str(ex), "Job Alert Loop", today, now)

    except Exception as ex:
        errorLog_file(str(ex), f"DriverMain - URL: {url}", today, now)

# Call the DriverMain function with job positions of interest
list_of_positions = ["pharmacy", "pharmaceutical", "Pharmavigilance"]
DriverMain(list_of_positions)
