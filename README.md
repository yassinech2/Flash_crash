 # Flash Crash Detection

This project focuses on the analysis of DIJA index during the crash of 2010 and an attempt to build model that predicts it. 
 The following is an organizational structure and explanation of each file within the project.

## File Organization

- `1_Data_Wrangling.ipynb`: Contains the data extraction and cleaning
- `2_Data_Exploration.ipynb`: Contains the data exploration
- `3_Data_Interpretation.ipynb`: Contains the data interpretation
- `4_Response_function.ipynb`: Contains the response function analysis and interpretation
- `5_Crash_Detection.ipynb`: Contains the test images that are input to the models to evaluate their segmentation capabilities.

- `helpers.py`: Contains utility functions and helpers used across different scripts in the project.
- `README.md`: Markdown file providing an overview and documentation for the project.


## Data
The data we used is tick by tick data of DIJA index for the year of the crash2010.
You can follow `1_Data_Wrangling.ipynb` for a demo of using this data.


### Training Models
We provide our 4 predictive models in `5_Crash_Detection.ipynb`. Some of the key findings in `4_Response_function.ipynb` might help to understand the rationale behind our models.
