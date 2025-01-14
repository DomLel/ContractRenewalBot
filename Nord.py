import os
import sqlite3
import json
from datetime import datetime, timedelta
from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError


class Contract:
    def __init__(self, name, owner, organization, annual_cost, contract_renewal):
        self.name = name
        self.owner = owner
        self.organization = organization
        self.annual_cost = annual_cost
        self.contract_renewal = contract_renewal


def log_error(message):
    slack_token = os.environ.get("SLACK_BOT_TOKEN")
    log_file = "error_log.txt"
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    # Format error message
    full_message = f"[{timestamp}] {message}\n"

    # Write to a local file
    with open(log_file, "a") as file:
        file.write(full_message)

    # Send error message to Slack
    if slack_token:
        client = WebClient(token=slack_token)
        try:
            client.chat_postMessage(channel="nordsec-test", text=f"Error occurred:\n{full_message}")
        except SlackApiError as e:
            # Log Slack API error to local file
            with open(log_file, "a") as file:
                file.write(f"[{timestamp}] Failed to send error to Slack: {e.response['error']}\n")


def validate_table_and_columns(cursor, table_name, columns):
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name=?;", (table_name,))
    if cursor.fetchone() is None:
        raise ValueError(f"Table '{table_name}' does not exist in the database.")
    
    cursor.execute(f"PRAGMA table_info({table_name});")
    existing_columns = {row[1] for row in cursor.fetchall()}
    for column in columns:
        if column not in existing_columns:
            raise ValueError(f"Column '{column}' does not exist in table '{table_name}'.")


def validate_input(limit, last_row):
    if not isinstance(limit, int) or limit <= 0:
        raise ValueError("Limit must be a positive integer.")
    if not isinstance(last_row, int) or last_row < 0:
        raise ValueError("Last row must be a non-negative integer.")


def validate_row_data(row):
    if not isinstance(row[2], (int, float)) or row[2] < 0:
        raise ValueError(f"Invalid annual cost: {row[2]}")
    if row[3]:
        try:
            datetime.strptime(row[3], "%Y-%m-%d")
        except ValueError:
            raise ValueError(f"Invalid date format for contract renewal: {row[3]}")


def fetch_contracts(contracts, db_path, table_name, name_column_name, owner_column_name, cost_column_name, renewal_column_name, limit, last_row=0):
    if not os.path.exists(db_path):
        raise FileNotFoundError(f"Database file '{db_path}' not found.")
    
    validate_input(limit, last_row)

    try:
        connection = sqlite3.connect(db_path)
        cursor = connection.cursor()

        validate_table_and_columns(cursor, table_name, [name_column_name, owner_column_name, cost_column_name, renewal_column_name])

        query = f"""
        WITH RankedContracts AS (
            SELECT
                ROW_NUMBER() OVER (ORDER BY ROWID) AS row_num,
                {name_column_name},
                {owner_column_name},
                {cost_column_name},
                {renewal_column_name}
            FROM
                {table_name}
        )
        SELECT
            {name_column_name},
            {owner_column_name},
            {cost_column_name},
            {renewal_column_name}
        FROM
            RankedContracts
        WHERE
            row_num > ? 
        LIMIT ?;
        """

        cursor.execute(query, (last_row, limit))
        rows = cursor.fetchall()

        for row in rows:
            validate_row_data(row)
            contract_renewal_date = datetime.strptime(row[3], "%Y-%m-%d").date() if row[3] else None
            contract = Contract(
                name=row[0],
                owner=row[1],
                organization=table_name,
                annual_cost=row[2],
                contract_renewal=contract_renewal_date
            )
            contracts.append(contract)

        if rows:
            return len(rows)
        return 0
    except sqlite3.Error as e:
        log_error(f"Database error: {e}")
        return 0
    finally:
        connection.close()


def generate_report(contracts):
    today = datetime.now().date()

    def check_conditions(contract):
        conditions = []
        if contract.contract_renewal and contract.contract_renewal <= today + timedelta(days=3):
            conditions.append("Needs renewal in the next 3 days")
        if contract.contract_renewal and today < contract.contract_renewal <= today + timedelta(days=14):
            conditions.append("Needs renewal in the next 14 days")
        if contract.contract_renewal and contract.contract_renewal <= today + timedelta(days=30) and contract.annual_cost >= 10000:
            conditions.append("High cost (€10,000 or more) renewal in the next 1 month")
        return conditions

    report = []
    for contract in contracts:
        conditions = check_conditions(contract)
        if conditions:
            report.append(
                f"Name: {contract.name}\n"
                f"Owner: {contract.owner}\n"
                f"Organization: {contract.organization}\n"
                f"Annual Cost: €{contract.annual_cost:,.2f}\n"
                f"Renewal Date: {contract.contract_renewal}\n"
                f"Conditions: {', '.join(conditions)}\n"
                f"------------------------"
            )

    return "\n".join(report)


def read_last_row(file_path):
    if os.path.exists(file_path):
        with open(file_path, "r") as file:
            return json.load(file)
    return {}


def write_last_row(file_path, last_row_data):
    with open(file_path, "w") as file:
        json.dump(last_row_data, file, indent=4)


def main():
    contracts = []
    db_path = "contracts.sqlite"
    last_row_file = "last_row.json"

    try:
        # Load last_row data
        last_row_data = read_last_row(last_row_file)

        tables = [
            {
                "table_name": "vertex_systems",
                "name_column_name": "name",
                "owner_column_name": "evangelist",
                "cost_column_name": "annual_cost",
                "renewal_column_name": "contract_renewal",
                "last_row": last_row_data.get("vertex_systems", 0),
                "limit": 1
            },
            {
                "table_name": "xtech_solutions",
                "name_column_name": "name",
                "owner_column_name": "owner",
                "cost_column_name": "cost",
                "renewal_column_name": "renewal_date",
                "last_row": last_row_data.get("xtech_solutions", 0),
                "limit": 1
            }
        ]

        for table in tables:
            rows_fetched = fetch_contracts(
                contracts=contracts,
                db_path=db_path,
                table_name=table["table_name"],
                name_column_name=table["name_column_name"],
                owner_column_name=table["owner_column_name"],
                cost_column_name=table["cost_column_name"],
                renewal_column_name=table["renewal_column_name"],
                limit=table["limit"],
                last_row=table["last_row"]
            )
            # Update last_row if rows were fetched
            last_row_data[table["table_name"]] = table["last_row"] + rows_fetched

        report = generate_report(contracts)

        slack_token = os.environ.get("SLACK_BOT_TOKEN")
        if not slack_token:
            raise EnvironmentError("SLACK_BOT_TOKEN environment variable is not set.")

        client = WebClient(token=slack_token)

        client.chat_postMessage(
            channel="nordsec-test",
            text=report
        )

        # Save updated last_row data
        write_last_row(last_row_file, last_row_data)

    except Exception as e:
        log_error(str(e))


if __name__ == "__main__":
    main()

#Improvements:
#Make the formatting nicer, improve comments
#Write up documentation in the repo landing page
#Make unit tests with import unittest
#File that assigns normal names according to table names