import time
import os
import csv
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.action_chains import ActionChains
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

    folder_path = r"C:\Users\ZaurTskhvaradze\Jenkins_Reports"
    file_name = "report.html"
    file_path = os.path.join(folder_path, file_name)

    with WebDriverContext() as driver:
        print(f"Opening file: {file_path}")
        driver.get(f"file:///{file_path}")

        wait = WebDriverWait(driver, 10)
        actions = ActionChains(driver)

        time.sleep(2)

        # ============================================================
        # STEP 2: Extract PLOTLY TABLE
        # ============================================================
        try:
            print("\nExtracting Plotly SVG Table...")
            time.sleep(2)

            # Extract table text
            script = """
                const table = document.querySelector('g.table');
                if (!table) return null;
                return Array.from(table.querySelectorAll('text')).map(t => t.textContent.trim());
            """
            texts = driver.execute_script(script)

            if not texts:
                raise Exception("Table not found")

            print(f"DEBUG: Extracted {len(texts)} raw text items")

            # Count columns
            script_cols = """
                return document.querySelectorAll('g.y-column').length;
            """
            col_count = driver.execute_script(script_cols)

            print(f"DEBUG: Detected {col_count} Plotly columns")

            rows_per_col = len(texts) // col_count
            columns = []

            for i in range(col_count):
                start = i * rows_per_col
                end = start + rows_per_col
                columns.append(texts[start:end])

            headers = [col[0] for col in columns]
            rows = list(zip(*[col[1:] for col in columns]))

            csv_path = os.path.join(folder_path, "table.csv")
            with open(csv_path, "w", newline="", encoding="utf-8") as f:
                writer = csv.writer(f)
                writer.writerow(headers)
                writer.writerows(rows)

            print(f"✔ Saved table.csv → {csv_path}")

        except Exception as e:
            print(f"❌ Table extraction failed: {e}")

        # ============================================================
        # STEP 3A: Locate chart container
        # ============================================================
        try:
            print("\nLocating doughnut chart...")
            chart = wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "div.js-plotly-plot")))
            print("✔ Doughnut chart container found.")
        except Exception as e:
            print(f"❌ Failed to locate doughnut chart: {e}")

        # ============================================================
        # STEP 3C: Count slices (actual filters)
        # ============================================================
        try:
            print("\nLocating doughnut chart slices...")
            script_slices = """
                return document.querySelectorAll('g.pielayer g.slice').length;
            """
            slice_count = driver.execute_script(script_slices)

            print(f"✔ Found {slice_count} slices")

            if slice_count == 0:
                raise Exception("No slices found")

        except Exception as e:
            print(f"❌ Failed to detect slices: {e}")

        # ============================================================
        # STEP 3D: Screenshot0 (initial)
        # ============================================================
        try:
            print("\nTaking screenshot0...")
            screenshot_path = os.path.join(folder_path, "screenshot0.png")
            driver.save_screenshot(screenshot_path)
            print(f"✔ Saved screenshot0 → {screenshot_path}")
        except Exception as e:
            print(f"❌ Failed to take screenshot0: {e}")

        # ============================================================
        # STEP 3E: Iterate slices → hover → screenshot → CSV
        # ============================================================
        try:
            print("\nIterating through slices with hover tooltips...")

            for i in range(slice_count):
                print(f"\n--- Processing slice {i} ---")

                # 1. Get slice center coordinates for hovering
                script_coords = f"""
                    const slices = document.querySelectorAll('g.pielayer g.slice');
                    const path = slices[{i}].querySelector('path.surface');
                    const r = path.getBoundingClientRect();
                    return {{x: r.x + r.width/2, y: r.y + r.height/2}};
                """
                coords = driver.execute_script(script_coords)
                x, y = coords['x'], coords['y']

                # 2. Move mouse to slice to trigger tooltip
                actions.move_by_offset(x, y).perform()
                time.sleep(0.7)

                # 3. ScreenshotN
                screenshot_path = os.path.join(folder_path, f"screenshot{i+1}.png")
                driver.save_screenshot(screenshot_path)
                print(f"✔ Saved screenshot{i+1} → {screenshot_path}")

                # 4. Move mouse back to prevent offset accumulation
                actions.move_by_offset(-x, -y).perform()

                # 5. Extract slice text
                script_extract = f"""
                    const slice = document.querySelectorAll('g.pielayer g.slice')[{i}];
                    const txt = slice.querySelector('g.slicetext text');
                    if (!txt) return null;
                    return txt.getAttribute('data-unformatted');
                """
                raw = driver.execute_script(script_extract)
                if not raw:
                    raise Exception(f"Could not read label for slice {i}")

                label, value = raw.split("<br>")
                print(f"✔ Slice data: {label}, {value}")

                # 6. Save CSV
                csv_path = os.path.join(folder_path, f"doughnut{i}.csv")
                with open(csv_path, "w", newline="", encoding="utf-8") as f:
                    writer = csv.writer(f)
                    writer.writerow(["Facility Type", "Min Average Time Spent"])
                    writer.writerow([label, value])

                print(f"✔ Saved CSV → {csv_path}")

        except Exception as e:
            print(f"❌ Slice iteration failed: {e}")
