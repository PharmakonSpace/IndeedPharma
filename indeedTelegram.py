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

load_dotenv()
# Configuration
TELEGRAM_BOT_TOKEN = os.environ.get('TELEGRAM_BOT_TOKEN')  # Replace with your bot token securely
TELEGRAM_CHAT_ID = os.environ.get('TELEGRAM_CHAT_ID')

SENT_ALERTS_FILE = "sent_alerts.csv"

def load_sent_alerts():
    """Load the list of already sent alerts from the CSV file."""
    if os.path.exists(SENT_ALERTS_FILE):
        return pd.read_csv(SENT_ALERTS_FILE)['ID'].tolist()
    else:
        return []

def save_sent_alert(id):
    """Save a new alert ID to the CSV file."""
    df = pd.DataFrame({"ID": [id]})
    file_exists = os.path.exists(SENT_ALERTS_FILE)
    with open(SENT_ALERTS_FILE, mode='a', newline='', encoding='utf-8') as f:
        df.to_csv(f, header=not file_exists, index=False)

# Date and time object
today = date.today()
now = datetime.now().strftime("%H:%M:%S")

# Data frame and series Utilities
template = {"Job Title": [], "Company Name": [], "Location": [], "Salaries": [], "Job Description": [], "Link": []}
errorTemplate = {"Error": [], "Location": [], "Date": [], "Time": []}
errorDataFrame = pd.DataFrame(errorTemplate)
Main_DataFrame = pd.DataFrame(template)

# Initialize variables
titles = []
names = []
locations = []
salaries = []
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
                print(f"Http Error: {errh}")
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
        salary_elements = soup.find_all("div", class_="css-1cvvo1b eu4oa1w0")
        description_elements = soup.find_all("div", class_="css-9446fg eu4oa1w0")
        job_link_elements = soup.find_all("a", class_="jcs-JobTitle css-jspxzf eu4oa1w0")

        print(f"Found {len(title_elements)} titles, {len(company_elements)} companies, {len(location_elements)} locations, {len(salary_elements)} salaries, {len(description_elements)} descriptions, {len(job_link_elements)} links")

        # Handle cases where no data is found
        if not any([title_elements, company_elements, location_elements, salary_elements, description_elements, job_link_elements]):
            print(f"Some elements are missing in the page content for URL: {url}")
            return

        titles.extend([title.text.strip() for title in title_elements])
        names.extend([name.text.strip() for name in company_elements])
        locations.extend([location.text.strip() for location in location_elements])
        salaries.extend([salar.text.strip() if salar else "N/A" for salar in salary_elements])
        job_descriptions.extend([" ".join([li.text.strip() for li in desc.find_all("li")]) if desc else "N/A" for desc in description_elements])
        links.extend([f"https://www.indeed.com{link.get('href')}" if link else "N/A" for link in job_link_elements])

        print("Success Job card data")
        return (titles, names, locations, salaries, job_descriptions, links)

    except Exception as ex:
        errorLog_file(str(ex), "jobCard Function Failed", today, now)
        print("An error occurred in jobCard: ", ex)


def createDataFrame():
    try:
        # Debugging prints
        print(f"Titles length: {len(titles)}")
        print(f"Names length: {len(names)}")
        print(f"Locations length: {len(locations)}")
        print(f"Salaries length: {len(salaries)}")
        print(f"Job Descriptions length: {len(job_descriptions)}")
        print(f"Links length: {len(links)}")

        # Check for mismatched lengths
        if len(titles) != len(names) or len(names) != len(locations) or len(locations) != len(salaries) or len(salaries) != len(job_descriptions) or len(job_descriptions) != len(links):
            for i in range(len(titles)):
                print(f"Index {i}: Title={titles[i] if i < len(titles) else 'N/A'}, Salary={salaries[i] if i < len(salaries) else 'N/A'}")

        # Ensure all lists have the same length
        length = len(titles)
        if not all(len(lst) == length for lst in [names, locations, salaries, job_descriptions, links]):
            raise ValueError("Mismatch in the length of job data lists")

        job_card_data = [
            {
                "Job Title": titles[i],
                "Company Name": names[i],
                "Location": locations[i],
                "Salaries": salaries[i] if i < len(salaries) else "N/A",
                "Job Description": job_descriptions[i],
                "Link": links[i]
            }
            for i in range(length)
        ]

        print("Data Frame successfully created")
        return job_card_data

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

        # Check if the error log file already exists
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
    global titles, names, locations, salaries, job_descriptions, links
    titles.clear()
    names.clear()
    locations.clear()
    salaries.clear()
    job_descriptions.clear()
    links.clear()

    sent_alerts = load_sent_alerts()  # Load already sent alerts

    url = None  # Initialize url variable

    try:
        for job_title in listOfposition:
            url = f"https://in.indeed.com/jobs?q={job_title}&l=India&from=searchOnDesktopSerp"
            jobCard(url)

            # Ensure data was retrieved before continuing
            if not titles or not names or not locations or not salaries or not job_descriptions or not links:
                print(f"No data retrieved for {job_title}. Skipping this job title.")
                continue  # Skip to the next job title

            # Send Telegram alert for each job title
            for i in range(len(titles)):
                try:
                    job_id = links[i].split("jk=")[-1].split("&")[0]

                    # Check if the job ID has already been alerted
                    if job_id not in sent_alerts:
                        message = (f"<b>Job Title:</b> {titles[i]}\n"
                                   f"<b>Company:</b> {names[i]}\n"
                                   f"<b>Location:</b> {locations[i]}\n"
                                   f"<b>Salaries:</b> {salaries[i]}\n"
                                   f"<b>Description:</b> {job_descriptions[i]}\n"
                                   f"<a href='{links[i]}'>Apply Here</a>")
                        send_telegram_alert(message)

                        # Save the alert ID to prevent duplicate alerts
                        save_sent_alert(job_id)
                        sent_alerts.append(job_id)  # Add ID to the list of sent alerts
                    else:
                        print(f"Alert already sent for job ID: {job_id}")

                except IndexError as ie:
                    print(f"IndexError for job title {titles[i]}: {ie}")
                    errorLog_file(str(ie), "IndexError in DriverMain", today, now)
                except Exception as ex:
                    print(f"An error occurred while processing job title {titles[i]}: {ex}")
                    errorLog_file(str(ex), "Error in processing job title", today, now)

            # Random sleep between 5 to 15 seconds
            time.sleep(random.uniform(5, 15))

        # Create and save the combined JSON after processing all job titles
        final_data = createDataFrame()
        if final_data is not None:
            with open("All_Jobs.json", 'w', encoding='utf-8') as f:
                json.dump(final_data, f, ensure_ascii=False, indent=4)
            print("All jobs data saved to All_Jobs.json")
        else:
            print("Failed to create combined JSON")

    except Exception as ex:
        if url:
            print("Failed link: " + url)
        print("An error occurred in DriverMain: ", ex)
        errorLog_file(str(ex), "DriverMain", today, now)

       
listOfposition = ["pharmacy", "pharmaceutical", "Pharmavigilance"]  # List of job titles to search
DriverMain(listOfposition)  # Driver function
