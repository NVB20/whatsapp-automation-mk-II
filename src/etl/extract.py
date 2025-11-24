from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.keys import Keys
from selenium.webdriver import ActionChains
from selenium.common.exceptions import TimeoutException
import time
from dotenv import load_dotenv
import os


def open_whatsapp():
    chrome_options = Options()
    chrome_options.add_argument("--disable-notifications")
    
    # Add session persistence
    user_data_dir = os.path.join(os.getcwd(), "whatsapp_session")
    chrome_options.add_argument(f"user-data-dir={user_data_dir}")
    
    driver = webdriver.Chrome(options=chrome_options)
    wait = WebDriverWait(driver, 30)

    try:
        print("Opening WhatsApp Web...")
        driver.get("https://web.whatsapp.com")

        # Check if already logged in by looking for the chat list
        try:
            wait.until(EC.presence_of_element_located(
                (By.CSS_SELECTOR, '#side [role="textbox"][contenteditable="true"]')
            ))
            print("Already logged in! Session restored successfully.")
            time.sleep(2)  
        except TimeoutException:
            print("Please scan QR code to login...")
            time.sleep(15)
        
        print("WhatsApp Web loaded successfully!")
        
        load_dotenv()
        group_name = os.getenv("GROUP_NAME")
        print(f"env group name: {group_name}")

        # --- Focus the LEFT SIDEBAR search box ---
        search_box = wait.until(EC.element_to_be_clickable(
            (By.CSS_SELECTOR, '#side [role="textbox"][contenteditable="true"]')
        ))
        search_box.click()
        search_box.send_keys(Keys.CONTROL, 'a')
        search_box.send_keys(Keys.BACK_SPACE)
        search_box.send_keys(group_name)

        # --- Select the first search result ---
        first_result = None
        try:
            results = WebDriverWait(driver, 5).until(
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
                results = WebDriverWait(driver, 5).until(
                    EC.presence_of_all_elements_located(
                        (By.XPATH, '//div[@role="listbox"]//div[@role="option"]')
                    )
                )
                if results:
                    first_result = results[0]
            except TimeoutException:
                pass

        if first_result:
            driver.execute_script("arguments[0].scrollIntoView({block:'center'});", first_result)
            driver.execute_script("arguments[0].click();", first_result)
        else:
            ActionChains(driver).send_keys(Keys.ARROW_DOWN).send_keys(Keys.ENTER).perform()

        print(f"Opened group: {group_name}")

        # --- Read messages from .env ---
        message_count = int(os.getenv("MESSAGE_COUNT", "20"))
        print(f"Reading last {message_count} messages...")
        
        # Wait for messages to load and scroll if needed
        max_attempts = 5
        attempt = 0
        
        while attempt < max_attempts:
            messages = driver.find_elements(By.CSS_SELECTOR, '[data-pre-plain-text]')
            
            if len(messages) < 5:
                print(f"Only {len(messages)} messages found. Waiting for more to load... (attempt {attempt + 1}/{max_attempts})")
                
                # Scroll up to load more messages
                try:
                    message_pane = driver.find_element(By.CSS_SELECTOR, "div[data-testid='conversation-panel-body']")
                    driver.execute_script("arguments[0].scrollTop = 0", message_pane)
                except Exception as e:
                    print(f"Could not scroll: {e}")
                
                time.sleep(3)
                attempt += 1
            else:
                print(f"Found {len(messages)} messages. Proceeding...")
                break
        
        # Final check
        messages = driver.find_elements(By.CSS_SELECTOR, '[data-pre-plain-text]')
        
        last_messages = messages[-message_count:] if len(messages) >= message_count else messages

        message_data = []
        for msg in last_messages:
            try:
                # Extract metadata 
                meta = msg.get_attribute("data-pre-plain-text")
                # Example: "[20:15, 25/08/2025] +972 50-123-4567: "
                if meta:
                    meta = meta.strip("[]")
                    timestamp, sender = meta.split("] ")[0], meta.split("] ")[1].replace(":", "")
                else:
                    timestamp, sender = "?", "?"

                # Extract message text (descendant spans)
                text_elems = msg.find_elements(By.CSS_SELECTOR, 'span.selectable-text span')
                text = " ".join([t.text for t in text_elems]) if text_elems else ""

                message_data.append({
                    "sender": sender,
                    "timestamp": timestamp,
                    "text": text
                })
            except Exception as e:
                print("Error reading message:", e)

        print(f"=== Last {message_count} messages ===")
        for m in message_data:
            print(m)

        print("Finished reading messages!")
        print(f"{len(message_data)} messages read")
        return message_data

    finally:
        driver.quit()