### Disclaimer
This repository represents a real-world project. The information contained within is solely for demonstration purposes, showcasing a successfully completed project. No sensitive data, including Personally Identifiable Information (PII), client information, or any other confidential materials, has been used or included in this repository. Any resemblance to real individuals, companies, or sensitive data is purely coincidental. Whenever possible, hypothetical data tables are used instead of real information.

### Objective 
Using Fabric User Data Functions Within A Data Pipeline.

### Technologies
Microsoft Fabric Analytics, Data Pipeline, On-premises Data Gateway, SQL Server 2022, Fabric SQL Database, Fabric User Data Functions

### Data Pipeline
<img width="780" alt="FabricUDF" src="https://github.com/user-attachments/assets/623d4d6b-5449-42d0-8fa7-a96ab96686f3" />

### Scenario
*	We are loading data from an On-premises SQL database into a Fabric SQL database via the On-premises data gateway.
*	Within Fabric, the existing tables in the Fabric SQL database are not accessible to everyone — only users with specific permissions can access them.
*	The Sales department has requested access to customer sales transactions. For reporting purposes, they need both customer details (for reference only) and customer spending amounts (must).

### Proposed Solution
*	Adding a new User Data Function (UDF) activity into the existing Fabric data pipeline.
•	Creating a new table in the Fabric SQL database specifically for the Sales department, and configure it with appropriate permissions.
•	Inside the UDF, performing data manipulations such as table joins, data aggregations, and masking sensitive information like email addresses. Then, inserting the refined data into the newly created table for the Sales team.

### Article
https://medium.com/@zaw-may/using-fabric-user-data-functions-within-a-data-pipeline-a308769e8f32
