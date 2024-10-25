## Infrastructure and Role Management Setup

1. DDL SQL and Role Management: The current setup isn't best practice. It was configured this way to get a Snowflake environment running quickly for development and testing.

2. Production Recommendations:
   - Infrastructure: Consider using tools like Terraform for better version control and easier management of Snowflake resources.
   - Role Management: The current setup mostly uses SYSADMIN for everything. In production, a more granular RBAC system would be needed to follow the principle of least privilege.

3. Rationale: These simplifications helped to get the project up and running fast. For any production deployment or when time allows for refactoring, these aspects should be revisited.

4. Next steps: As the project matures or moves towards production, proper cloud infrastructure management and security practices will need to be implemented.
