# E-mini S&P 500 Futures Tick Data Analysis

This project analyzes E-mini S&P 500 futures tick data, performing various transformations and statistical analyses.

## Documentation

- [Analysis](docs/analysis.md): Detailed analysis results, answering key questions about bar formation, serial correlation, variance, and normality tests.

- [Architecture](docs/architecture.md): Overview of the project's technical architecture, including data flow and transformation processes.

- [Instructions](docs/instructions.md): Step-by-step guide on how to set up and run the project, including dbt pipeline and analysis scripts.

- [Data Overview](docs/data_overview.md): Background information on E-mini S&P 500 futures contracts and their significance in financial markets.

## Getting Started

To get started with this project, please refer to the [Instructions](docs/instructions.md) document for setup and execution details.

For a comprehensive understanding of the analysis performed and its results, see the [Analysis](docs/analysis.md) document.

## Potential Improvements

The following areas have been identified for potential future enhancements:

- Review the analysis results with quantitative analysts to refine the tests and ensure correctness.
- Implement infrastructure management using Terraform for better scalability and maintainability.
- Establish proper Role-Based Access Control (RBAC) with least privilege principles to enhance security.
- Utilize AWS services for the extraction and load steps to improve efficiency and leverage cloud capabilities.
- Expand test coverage by implementing additional tests for downstream models in the fact schema, complementing the existing tests for staging models.
