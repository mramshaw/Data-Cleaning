#!/usr/bin/env python

"""
A quick and dirty 'cleaner' for some data files.

Three datasets will be cleaned, with cells reformatted as needed.
"""

import numpy as np
import pandas as pd

# First dataset ====================================

DF = pd.read_csv('Datasets/BL-Flickr-Images-Book.csv', skipinitialspace=True)

TO_DROP = ['Edition Statement',
           'Corporate Author',
           'Corporate Contributors',
           'Former owner',
           'Engraver',
           'Contributors',
           'Issuance type',
           'Shelfmarks']
DF.drop(TO_DROP, axis=1, inplace=True)

DF.set_index('Identifier', inplace=True)

# Use a regular expression to extract a cleaned-up Date of Publication
EXTRACT = DF['Date of Publication'].str.extract(r'^(\d{4})', expand=False)
DF['Date of Publication'] = pd.to_numeric(EXTRACT)

# Use numpy to clean up Place of Publication
PUB = DF['Place of Publication']
LONDON = PUB.str.contains('London')
OXFORD = PUB.str.contains('Oxford')
DF['Place of Publication'] = np.where(LONDON, 'London',
                                      np.where(OXFORD, 'Oxford',
                                               PUB.str.replace('-', ' ')))

DF.to_csv('Output/BL-Flickr-Images-Book.csv', header='column_names')

# Second dataset ===================================

UNIVERSITY_TOWNS = []
with open('Datasets/university_towns.txt') as towns:
    for line in towns:
        if '[edit]' in line:
            # Remember this `state` until the next is found
            state = line
        else:
            # Otherwise, we have a city; keep `state` as last-seen
            UNIVERSITY_TOWNS.append((state, line))

TOWNS_DF = pd.DataFrame(UNIVERSITY_TOWNS,
                        columns=['State', 'RegionName'])


def get_citystate(item):
    """Help for cleaning up data cells."""
    if ' (' in item:
        return item[:item.find(' (')]
    elif '[' in item:
        return item[:item.find('[')]
    return item


# Apply our function to each cell in our dataframe
TOWNS_DF = TOWNS_DF.applymap(get_citystate)

# Was TXT but probably CSV is a lot more useful
TOWNS_DF.to_csv('Output/university_towns.csv', header='column_names')

# Third dataset ====================================

# Our real header line is the second one (offset 1)
OLYMPICS_DF = pd.read_csv('Datasets/olympics.csv', header=1)

# The mapping of old -> new column names
NEW_NAMES = {'Unnamed: 0': 'Country',
             '? Summer': 'Summer Olympics',
             '01 !': 'Gold',
             '02 !': 'Silver',
             '03 !': 'Bronze',
             '? Winter': 'Winter Olympics',
             '01 !.1': 'Gold.1',
             '02 !.1': 'Silver.1',
             '03 !.1': 'Bronze.1',
             '? Games': '# Games',
             '01 !.2': 'Gold.2',
             '02 !.2': 'Silver.2',
             '03 !.2': 'Bronze.2'}

# Rename our columns
OLYMPICS_DF.rename(columns=NEW_NAMES, inplace=True)

OLYMPICS_DF.to_csv('Output/olympics.csv', header='column_names')
