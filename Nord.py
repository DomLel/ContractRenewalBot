""" Basic operations using Slack_sdk """

import os
from slack_sdk import WebClient 
from slack_sdk.errors import SlackApiError
import sqlite3

#Basic concept:
import sqlite3
from datetime import datetime, timedelta

# Contract class
class Contract:
    def __init__(self, name, category, sub_category, evangelist, annual_cost, contract_renewal):
        self.name = name  # string
        self.category = category  # string
        self.sub_category = sub_category  # string
        self.evangelist = evangelist  # string
        self.annual_cost = annual_cost  # int
        self.contract_renewal = contract_renewal  # date (as a string or datetime object)

    def __repr__(self):
        return (f"Contract(name={self.name}, category={self.category}, "
                f"sub_category={self.sub_category}, evangelist={self.evangelist}, "
                f"annual_cost={self.annual_cost}, contract_renewal={self.contract_renewal})")


# Function to fetch contracts with row number tracking
def fetch_contracts_with_row_tracking(db_path, table_name, limit, last_row=0):
    contracts = []
    try:
        connection = sqlite3.connect(db_path)
        cursor = connection.cursor()

        # Query with row number tracking
        query = f"""
        WITH RankedContracts AS (
            SELECT ROW_NUMBER() OVER (ORDER BY ROWID) AS row_num, 
                   name, category, sub_category, evangelist, annual_cost, contract_renewal
            FROM {table_name}
        )
        SELECT name, category, sub_category, evangelist, annual_cost, contract_renewal
        FROM RankedContracts
        WHERE row_num > {last_row}
        LIMIT {limit};
        """

        cursor.execute(query)
        rows = cursor.fetchall()

        for row in rows:
            contract_renewal_date = datetime.strptime(row[5], "%Y-%m-%d").date() if row[5] else None
            contract = Contract(
                name=row[0],
                category=row[1],
                sub_category=row[2],
                evangelist=row[3],
                annual_cost=row[4],
                contract_renewal=contract_renewal_date
            )
            contracts.append(contract)

        # Update the last row number
        if rows:
            last_row += len(rows)

    except sqlite3.Error as e:
        print(f"Database error: {e}")
    finally:
        connection.close()

    return contracts, last_row


# Function to filter urgent renewals
def get_urgent_renewals(contracts):
    today = datetime.now().date()
    urgent_threshold = today + timedelta(days=3)
    return [contract for contract in contracts if contract.contract_renewal and contract.contract_renewal <= urgent_threshold]


# Function to filter upcoming renewals
def get_upcoming_renewals(contracts):
    today = datetime.now().date()
    upcoming_threshold = today + timedelta(days=14)
    return [contract for contract in contracts if contract.contract_renewal and today < contract.contract_renewal <= upcoming_threshold]


# Function to filter high-cost renewals
def get_high_cost_renewals(contracts):
    today = datetime.now().date()
    high_cost_threshold = today + timedelta(days=30)
    return [contract for contract in contracts if contract.contract_renewal and contract.contract_renewal <= high_cost_threshold and contract.annual_cost >= 10000]


# Function to generate the formatted report
# Function to generate the updated report without grouping
def generate_report_no_grouping(contracts, table_name):
    today = datetime.now().date()

    # Define condition checks
    def check_conditions(contract):
        conditions = []
        if contract.contract_renewal and contract.contract_renewal <= today + timedelta(days=3):
            conditions.append("Needs renewal in the next 3 days")
        if contract.contract_renewal and today < contract.contract_renewal <= today + timedelta(days=14):
            conditions.append("Needs renewal in the next 14 days")
        if contract.contract_renewal and contract.contract_renewal <= today + timedelta(days=30) and contract.annual_cost >= 10000:
            conditions.append("High cost (€10,000 or more) renewal in the next 1 month")
        return conditions

    # Generate the report
    report = []
    for contract in contracts:
        conditions = check_conditions(contract)
        if conditions:
            report.append(
                f"Name: {contract.name}\n"
                f"Evangelist: {contract.evangelist}\n"
                f"Organization: {table_name}\n"
                f"Annual Cost: €{contract.annual_cost:,.2f}\n"
                f"Renewal Date: {contract.contract_renewal}\n"
                f"Conditions: {', '.join(conditions)}\n"
                f"------------------------"
            )

    return "\n".join(report)


# Example usage
db_path = "contracts.sqlite"  # Path to your SQLite database
table_name = "vertex_systems"  # Replace with your table name
limit = 10  # Number of rows to fetch at a time
last_row = 0  # Start at the beginning

# Fetch contracts and update the row tracker
contracts, last_row = fetch_contracts_with_row_tracking(db_path, table_name, limit, last_row)

# Generate and print the updated report
report = generate_report_no_grouping(contracts, table_name)
print(report)

# Print the updated last row number
print(f"Next start row: {last_row}")

# Send the message
slack_token = os.environ.get("SLACK_BOT_TOKEN")

# Creating an instance of the Webclient class
client = WebClient(token=slack_token)

try:
	# Posting a message in #random channel
	response = client.chat_postMessage(
    				channel="nordsec-test",
    				text=report)
except SlackApiError as e:
	print(e)
	assert e.response["error"]
	

#Improvements:
#Dump any errors to a log file.
    #Use import logging
#Validate data and table structure.
#Handle errors
#Make the formatting nicer, improve comments
#Write a documentation document
    #In the github repo I reckon
#Make unit tests.
    #use import unittest
#Profile photo for bot