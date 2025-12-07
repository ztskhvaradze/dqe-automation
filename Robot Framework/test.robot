*** Settings ***
Library    SeleniumLibrary
Library    helper.py

Suite Teardown   Close All Browsers

*** Variables ***
${REPORT_PATH}    ${CURDIR}${/}my_report.html
${PARQUET_DIR}    ${CURDIR}${/}parquet_data

*** Test Cases ***
Validate HTML Table Against Parquet
    Open Browser    about:blank    chrome
    Maximize Browser Window

    ${df_html}=        Extract Plotly Table    ${REPORT_PATH}
    ${df_parquet}=     Load Parquet Df        ${PARQUET_DIR}

    # SORT BOTH DF before comparing
    ${df_html}=        Sort Dataframe    ${df_html}    Facility Type    Visit Date
    ${df_parquet}=     Sort Dataframe    ${df_parquet}    Facility Type    Visit Date

    ${diff}=           Compare Dataframes    ${df_html}    ${df_parquet}

    Should Be Empty    ${diff}    Differences found between HTML and Parquet dataset.
