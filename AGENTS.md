# AGENTS.md

**AI Project Instruction File** Created: 2026-03-04

------------------------------------------------------------------------

## 1. Agent Role

You are an AI coding assistant working inside this project. You must
follow ALL rules defined in this document before generating or modifying
code.

------------------------------------------------------------------------

## 2. Core Principle

-   Do NOT change working code unnecessarily.
-   Only modify files related to the requested task.
-   Never delete existing features unless explicitly instructed.
-   Always explain what will change before applying modifications.

------------------------------------------------------------------------

## 3. Tech Stack

-   Python 3.10+
-   Flask
-   Pandas
-   SQLite / CSV
-   JavaScript (UI)
-   VS Code environment

------------------------------------------------------------------------

## 4. Project Structure (DO NOT MODIFY)

encar_ai/ ├ app.py ├ config/ ├ data/ ├ collectors/ ├ processors/ ├
predictors/ ├ ui/ └ logs/

Rules: - Data files → data/ - Scraping logic → collectors/ - Data
cleaning → processors/ - Prediction logic → predictors/ - UI only inside
ui/

------------------------------------------------------------------------

## 5. Coding Style

-   Follow PEP8
-   snake_case for functions
-   Descriptive variable names
-   Functions under 60 lines when possible
-   Add concise comments where needed

------------------------------------------------------------------------

## 6. Error Handling

-   Wrap external I/O with try/except
-   Log errors to logs/app.log
-   Never silently ignore exceptions

------------------------------------------------------------------------

## 7. Data Safety

-   Do NOT change database schema without approval
-   Use relative paths only
-   Do not overwrite raw data files

------------------------------------------------------------------------

## 8. Task Workflow

When given a task:

1.  Read AGENTS.md first
2.  Identify affected files
3.  Explain intended modification
4.  Apply minimal safe change
5.  Output only modified code sections

------------------------------------------------------------------------

## 9. Forbidden Actions

-   No large refactoring
-   No new external libraries
-   No structural changes
-   No hidden logic changes

------------------------------------------------------------------------

## 10. Completion Criteria

-   Code must run without syntax errors
-   Existing functionality must remain intact
-   All changes must align with project rules

END OF FILE
