# How to add a new merchant to PIM tables

1. Clone the repository and go with a terminal command line interface to this folder.

2. Add the new merchant name to `../macros/merchants_new.sql` or `../macros/merchants_active.sql` depending on the merchant status on this [PIM-controlled Google sheet](https://docs.google.com/spreadsheets/d/1qoMyAAgWpvaXCnR6oQzdBP8Rdz5_axki2uUTxY0XSkI/edit#gid=1689334287). 

3. Type/copy the following command to add a merchant (ex. here: linnepe) 
    ```bash
    export GFGH_NAME='linnepe' && bash create_tables.sh && python3 generate_schemas.py
    ```

4. Add, commit, and deploy all changes to GitHub and create a new release (with higher release version).
