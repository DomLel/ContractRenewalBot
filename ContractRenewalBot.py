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

def validate_row_data(row):
    if not isinstance(row[2], (int, float)) or row[2] < 0:
        raise ValueError(f"Invalid annual cost: {row[2]}")
    if row[3]:
        try:
            datetime.strptime(row[3], "%Y-%m-%d")
        except ValueError:
            raise ValueError(f"Invalid date format for contract renewal: {row[3]}")


def fetch_contracts(contracts, db_path, table_name, name_column_name, owner_column_name, cost_column_name, renewal_column_name):
    if not os.path.exists(db_path):
        raise FileNotFoundError(f"Database file '{db_path}' not found.")

    try:
        connection = sqlite3.connect(db_path)
        cursor = connection.cursor()

        validate_table_and_columns(cursor, table_name, [name_column_name, owner_column_name, cost_column_name, renewal_column_name])

        today = datetime.now().strftime("%Y-%m-%d")

        query = f"""
        WITH UpcomingContracts AS (
            SELECT
                {name_column_name},
                {owner_column_name},
                {cost_column_name},
                {renewal_column_name}
            FROM
                {table_name}
            WHERE
                {renewal_column_name} > ?
        )
        SELECT
            {name_column_name},
            {owner_column_name},
            {cost_column_name},
            {renewal_column_name}
        FROM
            UpcomingContracts
        """

        cursor.execute(query, (today,))
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

def read_table_config(file_path):
    if os.path.exists(file_path):
        try:
            with open(file_path, "r") as file:
                table_config = json.load(file)

                if not isinstance(table_config, list):
                    raise ValueError("The table configuration file must contain a list of table configurations.")

                for table in table_config:
                    required_keys = ["table_name", "name_column_name", "owner_column_name", "cost_column_name", "renewal_column_name"]
                    for key in required_keys:
                        if key not in table:
                            raise ValueError(f"Missing required key '{key}' in table configuration: {table}")

                return table_config
        except (json.JSONDecodeError, ValueError) as e:
            log_error(f"Error reading or validating table config: {e}")
            raise
    else:
        raise FileNotFoundError(f"Table configuration file '{file_path}' not found.")
    
def main():
    contracts = []
    db_path = "contracts.sqlite"
    table_config_file = "tables_config.json"

    try:
        table_config = read_table_config(table_config_file)

        for table in table_config:
            fetch_contracts(
                contracts=contracts,
                db_path=db_path,
                table_name=table["table_name"],
                name_column_name=table["name_column_name"],
                owner_column_name=table["owner_column_name"],
                cost_column_name=table["cost_column_name"],
                renewal_column_name=table["renewal_column_name"]
            )

        report = generate_report(contracts)

        slack_token = os.environ.get("SLACK_BOT_TOKEN")
        if not slack_token:
            raise EnvironmentError("SLACK_BOT_TOKEN environment variable is not set.")

        client = WebClient(token=slack_token)

        client.chat_postMessage(
            channel="nordsec-test",
            text=report
        )

    except Exception as e:
        log_error(str(e))


if __name__ == "__main__":
    main()