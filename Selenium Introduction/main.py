import time
import os
import csv
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC


# ----------------------------------------------
# 1. CONTEXT MANAGER FOR WEBDRIVER
# ----------------------------------------------
class WebDriverContext:
    def __init__(self, driver_path=None):
        self.driver = None
        self.driver_path = driver_path

    def __enter__(self):
        self.driver = webdriver.Chrome()
        self.driver.maximize_window()
        print("WebDriver started.")
        return self.driver

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.driver:
            self.driver.quit()
            print("WebDriver closed successfully.")


# ----------------------------------------------
# 2. MAIN EXECUTION
# ----------------------------------------------
if __name__ == "__main__":

    # Change path to your report.html
    folder_path = r"C:\Users\ZaurTskhvaradze\Jenkins_Reports"
    file_name = "report.html"
    file_path = os.path.join(folder_path, file_name)

    with WebDriverContext() as driver:
        print(f"Opening file: {file_path}")
        driver.get(f"file:///{file_path}")

        # Wait for Plotly to load
        time.sleep(2)

        # ============================================================
        # STEP 2: Extract PLOTLY SVG TABLE
        # ============================================================
        try:
            print("\nExtracting Plotly SVG Table...")

            # wait a bit for JS to render table
            time.sleep(2)

            # Extract all <text> elements inside the table
            script = """
                const table = document.querySelector('g.table');
                if (!table) { return null; }

                const texts = Array.from(table.querySelectorAll('text'))
                                .map(node => node.textContent.trim());
                return texts;
            """

            texts = driver.execute_script(script)

            if not texts:
                raise Exception("Could not locate Plotly table text via JS extraction")

            print(f"DEBUG: Extracted {len(texts)} raw text items")

            # Next, extract number of columns
            script_cols = """
                const cols = document.querySelectorAll('g.y-column');
                return cols.length;
            """
            col_count = driver.execute_script(script_cols)

            print(f"DEBUG: Detected {col_count} Plotly columns")

            if col_count == 0:
                raise Exception("Could not detect y-column groups in Plotly table")

            # Split texts into columns
            rows_per_col = len(texts) // col_count

            columns = []
            for i in range(col_count):
                start = i * rows_per_col
                end = start + rows_per_col
                columns.append(texts[start:end])

            # headers:
            headers = [col[0] for col in columns]

            # rows:
            rows = list(zip(*[col[1:] for col in columns]))

            # Save CSV
            csv_path = os.path.join(folder_path, "table.csv")
            with open(csv_path, "w", newline="", encoding="utf-8") as f:
                writer = csv.writer(f)
                writer.writerow(headers)
                writer.writerows(rows)

            print(f"✔ Table extracted successfully → {csv_path}")

        except Exception as e:
            print(f"❌ Table extraction failed: {e}")

        # ============================================================
        # STEP 3A: Locate doughnut chart container
        # ============================================================
        try:
            print("\nLocating doughnut chart...")

            wait = WebDriverWait(driver, 10)

            # Locate the chart (Plotly container)
            chart = wait.until(
                EC.presence_of_element_located(
                    (By.CSS_SELECTOR, "div.js-plotly-plot")
                )
            )

            print("✔ Doughnut chart container found.")

        except Exception as e:
            print(f"❌ Failed to locate doughnut chart: {e}")

        # ============================================================
        # STEP 3B: Locate doughnut chart filter items (legend entries)
        # ============================================================
        try:
            print("\nLocating doughnut chart filters...")

            # Filter items are inside the legend:
            script_filters = """
                const legendItems = document.querySelectorAll('g.legend g.legenditem, g.legend g.traces g.legend-item, g.legend g.legend-item');
                return Array.from(legendItems).map(item => item.textContent.trim());
            """

            filters = driver.execute_script(script_filters)

            if not filters or len(filters) == 0:
                raise Exception("No legend filter items found. The doughnut chart may not have interactive filters.")

            print(f"✔ Found {len(filters)} filter items: {filters}")

        except Exception as e:
            print(f"❌ Failed to locate doughnut chart filters: {e}")

        # ============================================================
        # STEP 3C: Locate all doughnut chart slices (actual filters)
        # ============================================================
        try:
            print("\nLocating doughnut chart slices (filters)...")

            script_slices = """
                const slices = document.querySelectorAll('g.pielayer g.slice');
                return slices.length;
            """
            slice_count = driver.execute_script(script_slices)

            print(f"✔ Found {slice_count} slices in the doughnut chart.")

            if slice_count == 0:
                raise Exception("No slices found in the doughnut chart.")

        except Exception as e:
            print(f"❌ Failed to detect slices: {e}")

        # ============================================================
        # STEP 3D: Take screenshot0 (initial doughnut chart state)
        # ============================================================
        try:
            print("\nTaking screenshot0 (initial chart)...")

            screenshot_path = os.path.join(folder_path, "screenshot0.png")

            # Take full-page screenshot
            driver.save_screenshot(screenshot_path)

            print(f"✔ Saved screenshot0 → {screenshot_path}")

        except Exception as e:
            print(f"❌ Failed to take screenshot0: {e}")

        # ============================================================
        # STEP 3E: Iterate through slices, click, screenshot, extract data
        # ============================================================
        try:
            print("\nIterating through doughnut slices...")

            # 1. Get all slices (JS returns actual DOM nodes)
            script_get_slices = """
                return Array.from(document.querySelectorAll('g.pielayer g.slice'));
            """
            slice_elements = driver.execute_script(script_get_slices)
            total_slices = len(slice_elements)

            if total_slices == 0:
                raise Exception("No slices found during iteration.")

            print(f"✔ Ready to iterate through {total_slices} slices")

            # 2. Loop through slices
            for i in range(total_slices):
                print(f"\n--- Processing slice {i} ---")

                # 2a. Click slice using JS
                script_click = f"""
                    const slices = Array.from(document.querySelectorAll('g.pielayer g.slice'));
                    const path = slices[{i}].querySelector('path.surface');
                    path.dispatchEvent(new MouseEvent('click', {{bubbles:true}}));
                """
                driver.execute_script(script_click)

                time.sleep(1)  # small delay so chart updates

                # 2b. Take screenshotN
                screenshot_path = os.path.join(folder_path, f"screenshot{i+1}.png")
                driver.save_screenshot(screenshot_path)
                print(f"✔ Saved screenshot → {screenshot_path}")

                # 2c. Extract label + value from the slice
                script_extract = f"""
                    const slice = document.querySelectorAll('g.pielayer g.slice')[{i}];
                    const txt = slice.querySelector('g.slicetext text');
                    if (!txt) return null;

                    const raw = txt.getAttribute('data-unformatted');
                    return raw;  // usually "Clinic<br>28"
                """
                raw_text = driver.execute_script(script_extract)

                if not raw_text:
                    raise Exception(f"Could not extract text from slice {i}")

                # Parse: "Clinic<br>28" → ["Clinic", "28"]
                parts = raw_text.split("<br>")
                label = parts[0]
                value = parts[1]

                print(f"✔ Extracted slice data: {label}, {value}")

                # 2d. Save CSV as doughnutN.csv
                csv_path = os.path.join(folder_path, f"doughnut{i}.csv")
                with open(csv_path, "w", newline="", encoding="utf-8") as f:
                    writer = csv.writer(f)
                    writer.writerow(["Facility Type", "Min Average Time Spent"])
                    writer.writerow([label, value])

                print(f"✔ Saved CSV → {csv_path}")

        except Exception as e:
            print(f"❌ Error during slice iteration: {e}")




