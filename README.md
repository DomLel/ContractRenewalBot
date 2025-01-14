# Contract renewal notification bot
This bot checks the contracts.sqlite database and sends a message to Slack to notify when contracts are nearing renewal.
A notification is generated if a contract fulfills one of these conditions:
* Urgent renewals: Contracts that need to be renewed within the next 3 days.
* Upcoming renewals: Contracts that need to be renewed within the next 14 days.
* High-cost renewals: Contracts with an annual cost of 10 000,00€ or more that need to be renewed within the next 1 month.
   
This is the format for a contract notification:  
*Name: Notion*  
*Owner: Taylor White*  
*Organization: vertex_systems*  
*Annual Cost: €32,226.40*  
*Renewal Date: 2025-01-21*  
*Conditions: Needs renewal in the next 14 days, High cost (€10,000 or more) renewal in the next 1 month*  

The script expects the files contracts.sqlite and tables_config.json. The first is the database,
the latter describes what the columns,
containing the values necessary for the notification, are called.

An example of the structure for tables_config.json:
~~~~
{
    "table_name": "vertex_systems",
    "name_column_name": "name",
    "owner_column_name": "evangelist",
    "cost_column_name": "annual_cost",
    "renewal_column_name": "contract_renewal",
    "limit": 1
}
~~~~
