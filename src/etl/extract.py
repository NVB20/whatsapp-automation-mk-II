from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.action_chains import ActionChains
from selenium.common.exceptions import TimeoutException
from dotenv import load_dotenv
import time
import os


def open_whatsapp_browser():
    """Open WhatsApp Web one time and return driver + wait"""
    chrome_options = Options()
    chrome_options.add_argument("--disable-notifications")

    # Persistent session
    user_data_dir = os.path.join(os.getcwd(), "whatsapp_session")
    chrome_options.add_argument(f"user-data-dir={user_data_dir}")

    driver = webdriver.Chrome(options=chrome_options)
    wait = WebDriverWait(driver, 30)

    print("Opening WhatsApp Web…")
    driver.get("https://web.whatsapp.com")

    # Check login
    try:
        wait.until(
            EC.presence_of_element_located(
                (By.CSS_SELECTOR, '#side [role="textbox"][contenteditable="true"]')
            )
        )
        print("Already logged in! Session restored.")
        time.sleep(2)
    except TimeoutException:
        print("Scan the QR code to login...")
        time.sleep(15)

    print("WhatsApp Web is ready.")
    return driver, wait



def open_group(driver, wait, group_name):
    """Open a WhatsApp group by name"""
    print(f"--- Opening group: {group_name} ---")

    # Focus search box
    search_box = wait.until(
        EC.element_to_be_clickable(
            (By.CSS_SELECTOR, '#side [role="textbox"][contenteditable="true"]')
        )
    )
    search_box.click()
    search_box.send_keys(Keys.CONTROL, 'a')
    search_box.send_keys(Keys.BACK_SPACE)
    search_box.send_keys(group_name)

    # Select first result
    first_result = None
    try:
        results = WebDriverWait(driver, 0.5).until(
            EC.presence_of_all_elements_located(
                (By.CSS_SELECTOR, 'div[data-testid="cell-frame-container"]')
            )
        )
        if results:
            first_result = results[0]
    except TimeoutException:
        pass

    if not first_result:
        try:
            results = WebDriverWait(driver, 0.5).until(
                EC.presence_of_all_elements_located(
                    (By.XPATH, '//div[@role="listbox"]//div[@role="option"]')
                )
            )
            if results:
                first_result = results[0]
        except TimeoutException:
            pass

    if first_result:
        driver.execute_script(
            "arguments[0].scrollIntoView({block:'center'});", first_result
        )
        driver.execute_script("arguments[0].click();", first_result)
    else:
        ActionChains(driver).send_keys(Keys.ARROW_DOWN).send_keys(Keys.ENTER).perform()

    print(f"Group opened: {group_name}")



def read_messages(driver, message_count):
    """Read last N WhatsApp messages"""
    print(f"Reading last {message_count} messages...")

    # Load messages
    max_attempts = 5
    for attempt in range(max_attempts):
        messages = driver.find_elements(By.CSS_SELECTOR, '[data-pre-plain-text]')
        if len(messages) < 5:
            print(f"Found {len(messages)} messages, loading more… ({attempt+1}/{max_attempts})")
            try:
                panel = driver.find_element(By.CSS_SELECTOR, "div[data-testid='conversation-panel-body']")
                driver.execute_script("arguments[0].scrollTop = 0", panel)
            except:
                pass
            time.sleep(3)
        else:
            break

    messages = driver.find_elements(By.CSS_SELECTOR, '[data-pre-plain-text]')
    last_messages = messages[-message_count:] if len(messages) >= message_count else messages

    data = []
    for msg in last_messages:
        try:
            meta = msg.get_attribute("data-pre-plain-text")
            if meta:
                meta = meta.strip("[]")
                timestamp, sender = meta.split("] ")[0], meta.split("] ")[1].replace(":", "")
            else:
                timestamp, sender = "?", "?"

            # WhatsApp Web 2025+ message text selector
            text_elems = msg.find_elements(
                By.CSS_SELECTOR,
                'span[dir="ltr"], span[dir="rtl"]'
            )

            text = " ".join(t.text for t in text_elems).strip()


            data.append({
                "sender": sender,
                "timestamp": timestamp,
                "text": text
            })
        except Exception as e:
            print("Error:", e)

    print(f"{len(data)} messages read.")
    return data


def run_multi_group_reader():
    load_dotenv()

    STUDENTS = os.getenv("STUDENTS_GROUP")
    SALES = os.getenv("SALES_TEAM_GROUP")
    MESSAGE_COUNT = int(os.getenv("MESSAGE_COUNT"))

    driver, wait = open_whatsapp_browser()

    try:
        # --- Group 1 ---
        open_group(driver, wait, STUDENTS)
        students_messages = read_messages(driver, MESSAGE_COUNT)
        print(students_messages)
        # --- Group 2 ---
        open_group(driver, wait, SALES)
        sales_messages = read_messages(driver, MESSAGE_COUNT)

        print("=== Done Extracting Messages ===")
        return {
            "students": students_messages,
            "sales": sales_messages
        }

    finally:
        driver.quit()