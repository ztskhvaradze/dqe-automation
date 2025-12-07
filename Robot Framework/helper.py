import os
import pandas as pd
import pyarrow.parquet as pq
from robot.libraries.BuiltIn import BuiltIn


def _get_driver():
    """Get the active Selenium WebDriver instance from SeleniumLibrary."""
    selenium_lib = BuiltIn().get_library_instance('SeleniumLibrary')
    return selenium_lib.driver


from robot.libraries.BuiltIn import BuiltIn
import pandas as pd
import time

def Extract_Plotly_Table(report_path):
    selenium = BuiltIn().get_library_instance("SeleniumLibrary")
    driver = selenium.driver

    driver.get("file:///" + report_path.replace("\\", "/"))
    time.sleep(1)

    # Extract ALL SVG text nodes
    nodes = driver.find_elements("css selector", "g.table text")
    texts = [n.text.strip() for n in nodes if n.text.strip()]

    if len(texts) < 10:
        raise ValueError("Extracted too few text nodes: " + str(texts))

    # Locate headers dynamically
    try:
        i0 = texts.index("Facility Type")
        i1 = texts.index("Visit Date")
        i2 = texts.index("Average Time Spent")
    except ValueError:
        raise ValueError("Could not find all expected headers in: " + str(texts))

    # Values appear BEFORE each header
    col1 = texts[0:i0]
    col2 = texts[i0+1:i1]
    col3 = texts[i1+1:i2]

    # Sanity check (they must align)
    row_count = min(len(col1), len(col2), len(col3))

    df = pd.DataFrame({
        "Facility Type": col1[:row_count],
        "Visit Date": col2[:row_count],
        "Average Time Spent": pd.to_numeric(col3[:row_count], errors="coerce")
    })

    return df


def load_parquet_df(parquet_dir):
    """
    Reads all .parquet files from the given directory and concatenates them into a single DataFrame.
    """
    if not os.path.exists(parquet_dir):
        raise FileNotFoundError(f"Parquet directory not found: {parquet_dir}")

    parquet_files = [
        os.path.join(parquet_dir, f)
        for f in os.listdir(parquet_dir)
        if f.endswith(".parquet")
    ]

    if not parquet_files:
        raise FileNotFoundError("No Parquet files found in directory.")

    dfs = [pq.read_table(path).to_pandas() for path in parquet_files]
    return pd.concat(dfs, ignore_index=True)


def filter_parquet_by_date(df, filter_date):
    """
    Filters Parquet DataFrame by Visit Date if filter_date is provided.
    """
    if filter_date:
        if "Visit Date" not in df.columns:
            raise ValueError("Parquet missing 'Visit Date' column.")
        return df[df["Visit Date"] == filter_date]
    return df


def Compare_Dataframes(df_html, df_parquet):

    from robot.libraries.BuiltIn import BuiltIn

    logger = BuiltIn().log_to_console

    # Force same column order
    df_parquet = df_parquet[df_html.columns]

    # Normalize dtypes
    df_html["Average Time Spent"] = df_html["Average Time Spent"].astype(float)
    df_parquet["Average Time Spent"] = df_parquet["Average Time Spent"].astype(float)

    # Sort both tables
    sort_cols = ["Facility Type", "Visit Date", "Average Time Spent"]
    df_html = df_html.sort_values(sort_cols, ignore_index=True)
    df_parquet = df_parquet.sort_values(sort_cols, ignore_index=True)

    differences = []

    # Different shape?
    if df_html.shape != df_parquet.shape:
        msg = f"Shape mismatch: HTML={df_html.shape}, PARQUET={df_parquet.shape}"
        logger(f"\n{msg}\n")
        differences.append(msg)
        return differences

    # Compare cell-by-cell
    for r in range(len(df_html)):
        for c in df_html.columns:
            html_val = df_html.loc[r, c]
            parquet_val = df_parquet.loc[r, c]

            if html_val != parquet_val:
                diff_msg = (
                    f"Row {r}, Column '{c}': "
                    f"HTML={html_val} | PARQUET={parquet_val}"
                )
                differences.append(diff_msg)

    # Print differences clearly
    if differences:
        logger("\n=== DATAFRAME DIFFERENCES FOUND ===")
        for d in differences:
            logger(d)
        logger("=== END OF DIFFERENCES ===\n")
        return differences

    # No differences → PASS
    logger("No differences found. DataFrames match.\n")
    return []



def save_html_table_as_parquet(html_report_path, output_parquet_path):
    """
    Loads HTML using Selenium (through SeleniumLibrary driver),
    extracts Plotly table, and saves it as a Parquet file.
    """
    df = extract_plotly_table(html_report_path)

    # Create directory if needed
    os.makedirs(os.path.dirname(output_parquet_path), exist_ok=True)

    df.to_parquet(output_parquet_path, index=False)

    return f"Saved parquet to: {output_parquet_path}"


from robot.api.deco import keyword

@keyword
def sort_dataframe(df, *columns):
    """Sort a pandas DataFrame by given columns and reset index."""
    if not columns:
        raise ValueError("You must provide at least one column to sort by.")
    return df.sort_values(list(columns)).reset_index(drop=True)


@keyword
def sort_dataframe(df, *columns):
    """Sort pandas DataFrame by given columns (numeric-safe) and reset index."""
    if not columns:
        raise ValueError("You must provide at least one column to sort by.")

    # convert date + numeric columns safely
    for col in columns:
        # auto-convert numeric-looking columns
        df[col] = pd.to_numeric(df[col], errors='ignore')
        # auto-convert date-looking columns
        try:
            df[col] = pd.to_datetime(df[col], errors='ignore')
        except:
            pass

    return df.sort_values(list(columns)).reset_index(drop=True)
