**Invoice Validation & Analytics Chatbot**

Overview

This project automates invoice validation using Databricks and provides a natural language chatbot interface to analyze invoice data.

The system extracts financial information from invoice PDFs, validates totals, stores results in a structured table, and allows users to ask questions such as:

How many invoices were flagged?
How many invoices passed validation?
What is the average tax amount?
Which invoice has the highest total?
Show invoices with missing shipping charges.

The chatbot converts natural language questions into SQL using Databricks-managed LLMs and executes them against the invoice validation dataset.
