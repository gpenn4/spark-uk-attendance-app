# England School Absence Analysis

## Table of Contents
- [Repository](#Repository-Structure)
- [Installation](#Installation)
- [Usage](#Usage)
- [Dataset](#dataset)
- [Notes](#Notes)

### Repository
```
├── README.md                     
├── requirements.txt    
│          
├── data/                         
│   └── Absence_3term201819_nat_reg_la_sch.csv
│
├── src/
│   ├── main.py
│   ├── queries.py
│   └── util.py
```

### Installation
1. Create a virtual environment:
    ```bash
    $  python3 -m venv my-env
    ```
    python3 -m venv pyspark
2. Activate the environment:
    ```bash
    $  . pyspark/bin/my-env
    ```
3. Install dependencies using requirements.txt:
    ```bash
    $  pip install -r requirements.txt
    ```

### Usage
1. Open the terminal, navigate into the source folder, and activate the virtual environment.
    ```bash
    $  cd spark-uk-attendance-app/src
    $  source my-env/bin/activate
    ```
2. Run the application:
    ```bash
    $  python main.py
    ```
3. Use the menu by typing the number corresponding to the task you want to perform (1-6), and then press 'return'.
4. To exit the program from the main menu, type '7' and then press enter.

### Dataset
:copyright: Crown Copyright, 2020, Department for Education \
This dataset [Absence_3term201819_nat_reg_la_sch.csv](https://explore-education-statistics.service.gov.uk/data-catalogue/data-set/097fd311-d368-4a12-ac38-45efab3f3f95) is licensed under the Open Government License 3.0.
<a href="https://www.nationalarchives.gov.uk/doc/open-government-licence/" rel="license”>Open Government Licence</a>

### Notes
- Years entered refer to the academic year starting in that year. 
    e.g. 2008 refers to the 2008-2009 school year.
- All text input commands must be followed by hitting the 'enter' key.
- You must exit out of the graphs/ charts before moving on with the analysis.