## Root Cause Analysis
Wind turbines (WTs) are complex structures that are required to operate under varying weather conditions. Their blades must be able to withstand hurricane-level winds, and gusts with sudden direction changes. Additionally, they must be stiff enough to avoid high deflections that would lead to blade-tower impacts. It is therefore understood that WTs are dynamic systems that face complex aeroelastic phenomena. Edgewise vibration is such a phenomenon. <sup>1</sup>

The primary modes of vibration of wind turbine blades are in-plane (primarily edgewise) and out-of-plane (primarily flapwise). These two modes of vibration are coupled, as the blades are structurally pre-twisted in general. Besides, owing to the coupling of the aerodynamic loads in the in-plane and out-of-plane directions, the responses (in-plane and out-of-plane) of the blades are coupled too. This coupling gives rise to nonlinearity due to aero-elasticity, as the loads are dependent on the response of the flexible blades. Of the two modes, the edgewise mode has very low or almost no aerodynamic damping, whereas the flapwise mode is aerodynamically damped in nature. Thus, the edgewise modes are of concern, as they may induce instabilities, while the flapwise modes contribute to fatigue damage.<sup>8</sup>
- ![Alt text](https://github.com/BBartee75/Blade_Edgewise_possible_damage_dection/blob/main/edgeVflap.jpg)

Edgewise blade deflections can cause localized stress and loading near the middle 1/3 of the blade. The stress causes the shell to buckle or fracture in the chord wise direction between the TE and spar shell bondlines. When the fracture reaches the reinforced area of the shell over the spar, the fracture propagates and follows the spar bondline spanwise in both directions

## Let me explain this information in simpler terms:

### What are Edgewise Vibration Conditions?

Edgewise vibrations happen when wind turbine blades move up and down in a certain way. These movements can sometimes damage the blades. Here's what you need to know:

1. Wind Turbine Blades Move
   - Wind turbine blades don't just stay still. They move because of the wind pushing on them.
   - There are different ways blades can move: forward-backward, side-to-side, and up-down.
   - Wind turbine blade cross sections
      - ![Alt text](https://github.com/BBartee75/Blade_Edgewise_possible_damage_dection/blob/main/bladepic.jpg)

2. Edgewise Movement
   - Edgewise means moving from one side to the other along the length of the blade.
   - Imagine taking a long ruler and bending it back and forth along its length.

3. Why Does This Happen?
   - Sometimes, the wind isn't blowing straight onto the blade.
   - This creates a problem called "yaw error," which is like the blade being slightly off course.
   - If the wind comes in at an angle, it can make the blade wobble more.
      
4. How Much Is Too Much?
   - Usually, this isn't a big deal, but if the blade moves too much (more than 15 degrees), it can get damaged.
   - Think of it like a seesaw. If you push it too far, it might break.

5. What Happens When It's Too Much?
   - The blade can get stressed and even crack along its length.
   - This creates a special kind of damage called a "T-shaped" fracture.
      - ![Alt text](https://github.com/BBartee75/Blade_Edgewise_possible_damage_dection/blob/main/damageDesc.jpg)
   - It looks like a big crack that goes across the blade and down its side.
      - ![Alt text](https://github.com/BBartee75/Blade_Edgewise_possible_damage_dection/blob/main/possibledamage.jpg)
   
6. Other Factors That Can Help
   - If the turbine is stopped or idle (not spinning), it's more likely to happen.
   - Wind speeds above 10 meters per second (about 22 mph) can also make this more likely.

Edgewise vibrations in wind turbine blades is a complex phenomenon which may be caused during periods in which the turbine is experiencing a yaw error (difference between nacelle heading and dominant wind direction) greater than 15 degrees during operation.  By summing the operational time during these periods, it is possible to determine the exposure of each turbine to this possible blade damage mechanism.   

## So what does this Python Script do?
I take a very highlevel apporach to use 10 min data (note: this would be better with high resultion data). I use the 10 min data to analyze the possibilty of a wind turbine blade in is a condtion that could cuase edgewise damage.

### Edgewise Data Conditions Analysis

This repository contains code for analyzing wind turbine data focusing on Edgewise conditions. We'll go through the key steps and considerations in this analysis.

### 1. Data Retrieval and Filtering
- Table 1 describes the functions used to evaluate each 10-minute time stamp in the dataset against the 15 degree yaw error threshold...
- ![Alt text](https://github.com/BBartee75/Blade_Edgewise_possible_damage_dection/blob/main/Screenshot%202025-01-17%20123612.jpg)

We pull data from SQL Server, examining the following data points:

- YEAR(A.PCTimeStamp) as Year
- MONTH(A.PCTimeStamp) as Month
- DAY(A.PCTimeStamp) as Day
- A.PCTimeStamp
- A.Amb_Temp_Avg
- A.Amb_WindSpeed_Max
- A.Amb_WindSpeed_Avg
- A.Amb_WindDir_Abs_Avg
- A.Nac_Direction_Avg
- A.Grd_Prod_Pwr_Avg
- A.Sys_Logs_FirstActAlarmNo
- A.Blds_PitchAngle_Avg

We filter all data where wind speed average is greater than 8 m/s.

### 2. Icing Condition Detection

Once data is ingested, we apply a column for possible icing based on the following criteria:

- Temperature <= 3°C
- Power output 15% less than OEM power curve output per wind bin
- Temperatures between 3°C and -17°C
- No active first alarms
- Blade angle less than 25 degrees

### 3. Yaw Error Calculation

We calculate yaw error and add a 'YawError' column:

- Uses absolute difference calculation for consistency
- Handles cases where one direction is ahead of the other by more than 180 degrees
- Logic: if difference > 180 degrees, subtracts difference from 360 degrees

### 4. 10-Minute Interval Filtering

Filters the DataFrame to retain only 10-minute intervals in chronological order:

- Uses groupby operations to handle data for each WTG separately
- Employs pandas functions like sort_values, groupby, diff, and isclose for efficient processing
- Handles potential issues with missing data or irregular intervals

### 5. Stall Condition Detection

Checks if previous row for Nac_Direction_Avg is within 2 degrees:

- Calculates absolute difference in 'Nac_Direction_Avg' between consecutive rows
- Marks rows where difference is ≤ 2 degrees as possible stall conditions

### 6. Edgewise Possibility Identification

Adds a new column 'Edgewise_Poss':

- Based on yaw error (> 15 degrees), valid intervals, and stall indicators
- Identifies situations where the turbine might be operating near its limits

### 7. Final Filtering

Filters rows based on:
- Edgewise possibility (Edgewise_Poss) is true
- Yaw error is 15 degrees or higher

### 8. Additional Calculations

#### Angle Quartile
Creates a new 'Angle_Quartile' column categorizing yaw error angles into predefined quartiles.

#### Hours Column
Creates a new 'Hours' column to categorize durations in minutes into predefined hour categories.

#### Damage Risk Assessment
Creates a new 'Damage_Risk' column assessing the risk associated with wind turbine operation based on wind speed and yaw error angle.

### 9. Output DataFrames

After running all code, we generate three main dataframes:

1. Edgewise_result: Shows WTGs in possible Edgewise condition
2. Missing_df: Displays dates not found in data due to possible data/power outages
3. data_10Min: Lists 10-minute data corresponding to possible Edgewise condition

This analysis aims to identify wind turbines in potential Edgewise damage conditions, providing insights into turbine health and operational efficiency.

Remember, wind turbines are complex machines designed to work in tough conditions. While edgewise vibrations can cause problems, engineers work hard to design them to withstand most situations.

Citations:
[1] https://windpowerlab.com/edgewise-vibrations/
[2] https://www.sciencedirect.com/topics/engineering/edgewise-mode
[3] https://iopscience.iop.org/article/10.1088/1742-6596/524/1/012037/pdf
[4] https://www.sciencedirect.com/science/article/abs/pii/S0022460X1400491X
[5] https://www.researchgate.net/figure/Edge-wise-and-flap-wise-vibrations-of-the-blade-Adapted-from-7_fig2_267825087
[6] https://xray.greyb.com/wind-turbines/reduce-vibrations-in-wind-turbine
[7] https://orbit.dtu.dk/files/275804924/5.0088036.pdf
[8] https://royalsocietypublishing.org/doi/10.1098/rsta.2014.0069
[9] https://forums.nrel.gov/t/edgewise-bending-moment-unidentified-vibration/766
