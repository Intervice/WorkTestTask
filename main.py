import asyncio, json, os, datetime, logging
from playwright.async_api import async_playwright
from apscheduler.schedulers.asyncio import AsyncIOScheduler

async def extract_data_from_row(row_locator):
    try:
        name_element = row_locator.locator("div:nth-child(1) a")
        name = await name_element.text_content() if await name_element.is_visible() else "N/A"
        name = name.strip()

        protocols_element = row_locator.locator("div:nth-child(2)")
        protocols = await protocols_element.text_content() if await protocols_element.is_visible() else "N/A"
        protocols = protocols.strip()

        tvl_element = row_locator.locator("div:nth-child(7)")
        tvl = await tvl_element.text_content() if await tvl_element.is_visible() else "N/A"
        tvl = tvl.strip()

        return {
            "Name": name,
            "Protocols": protocols,
            "TVL": tvl
        }
    except Exception as ex:
        logging.error(f"Error on stage of extracting data from element: {ex}")
        return


async def scrape_data():
    async with (async_playwright() as apw):
        browser = await apw.firefox.launch(headless=False, slow_mo=20)
        page = await browser.new_page()
        await page.goto("https://defillama.com/chains")

        distance = 500
        first_div = page.locator("#table-wrapper > div:first-child")
        last_div = page.locator("#table-wrapper > div:last-child")

        page_height = await page.evaluate("document.body.scrollHeight")
        window_height = await page.evaluate("window.innerHeight")

        await first_div.scroll_into_view_if_needed()
        await page.mouse.wheel(delta_x=0, delta_y=50 * 6)
        date = "Scraping at" + datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        scraped_data = {date: {"data_list": []}}
        unique_names = set()
        attempt = 3

        while True:
            visible_rows_locators = last_div.locator("div[style*='position: absolute'][style*='transform: translateY']")
            num_visible_rows = await visible_rows_locators.count()

            for i in range(num_visible_rows):
                row_locator = visible_rows_locators.nth(i)

                if await row_locator.locator("a[href*='/chain/']").count() > 0:
                    chain_data = await extract_data_from_row(row_locator)
                    if chain_data and chain_data["Name"] != "N/A":
                        if chain_data["Name"] not in unique_names:
                            unique_names.add(chain_data["Name"])
                            logging.info(f"Element '{chain_data["Name"]}' was added to dict")
                            scraped_data[date]["data_list"].append(chain_data)

            await page.mouse.wheel(delta_x=0, delta_y=distance)

            current_scroll_position = await page.evaluate("window.scrollY")

            if current_scroll_position + window_height >= page_height:
                logging.info(f"Scrolling reached the end of page.")
                if attempt <= 0:
                    break
                else:
                    attempt -= 1

        logging.info(f"Parsing was finished. Collected {len(scraped_data[date]["data_list"])} elements.")
        await browser.close()
        return scraped_data


# async def main():
#     full_scraped_result = await scrape_data()
#
#     try:
#         with open(r"/output/result.json", "a", encoding='utf-8') as file:
#             json.dump(full_scraped_result, file, ensure_ascii=False, indent=4)
#         logging.info("Data was saved in file.")
#     except Exception as e:
#         logging.error(f"Error on stage of dumping data to json file: {e}")
async def main():
    try:
        scraped_result = await scrape_data()

        output_path = "output/result.json"

        all_results = []
        if os.path.exists(output_path):
            try:
                with open(output_path, "r", encoding='utf-8') as file:
                    existing_data = json.load(file)

                    if isinstance(existing_data, list):
                        all_results = existing_data
                    else:
                        all_results = [existing_data]
            except json.JSONDecodeError:
                logging.warning(f"Existing {output_path} is empty or corrupted JSON. Starting with a new list.")
                all_results = []
            except Exception as e:
                logging.error(f"Error reading existing data from {output_path}: {e}", exc_info=True)
                all_results = []

        all_results.append(scraped_result)

        try:
            with open(output_path, "w", encoding='utf-8') as file:
                json.dump(all_results, file, ensure_ascii=False, indent=4)
            logging.info(f"New data appended and saved to {output_path}")
        except Exception as e:
            logging.error(f"Error on stage of dumping data to json file {output_path}: {e}", exc_info=True)
    except Exception as e:
        logging.critical(f"Error in main() function: {e}", exc_info=True)


def load_config():
    config_file_path = "config.json"
    default_config = {
        "interval_minutes": 5,
        "proxy_settings": {
            "enabled": False,
            "server": "",
            "username": "",
            "password": ""
        }
    }

    config = default_config
    try:
        if os.path.exists(config_file_path):
            with open(config_file_path, "r") as f:
                loaded_config = json.load(f)
                config.update(loaded_config)
                if "proxy_settings" in loaded_config and isinstance(loaded_config["proxy_settings"], dict):
                    config["proxy_settings"].update(loaded_config["proxy_settings"])
            logging.info(f"Config loaded from {config_file_path}")
        else:
            logging.warning(f"Config file not found: {config_file_path}. Using default configuration.")
            os.makedirs(os.path.dirname(config_file_path), exist_ok=True)
            with open(config_file_path, 'w', encoding='utf-8') as f:
                json.dump(default_config, f, indent=4)
            logging.info(f"Created default config file: {config_file_path}. Please review and configure it.")
    except Exception as e:
        logging.error(f"Error loading config: {e}. Using default configuration.", exc_info=True)
    return config


async def main_scheduler():
    config = load_config()
    interval = config.get("interval_minutes", 5)

    scheduler = AsyncIOScheduler()
    scheduler.add_job(main, 'interval', minutes=interval, id='defillama_scraper_job')
    logging.info(f"Scheduler was started with interval of {interval} minutes.")
    scheduler.start()

    try:
        while True:
            await asyncio.sleep(60)
    except (KeyboardInterrupt, SystemExit):
        logging.info("Program was stopped by user.")
        scheduler.shutdown()


if __name__ == "__main__":
    logging.basicConfig(filename=r"logs/scraper.log",
                        filemode="a",
                        format='%(asctime)s - %(levelname)s - %(message)s',
                        level=logging.INFO)
    asyncio.run(main_scheduler())
