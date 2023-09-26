# %%
## Import Libraries
import numpy as np
import pandas as pd
from urllib.parse import urlparse #we use this to extract domain extension from URL columns
import re as re

# %%
## Import Data and Preparing Dataframes
original_df = pd.read_csv('sheet.csv')

main_df = original_df.copy()

emails_df = main_df.copy()
category_df  = main_df.copy()

# %%
## Part 1 - Split the email column to multiple email_ columns

# Split, explode, generate email_number and pivot directly
emails_df = emails_df.assign(email=emails_df['email'].str.split(',')).explode('email').assign(
    email_number=lambda x: 'email_' + (x.groupby('ID').cumcount() + 1).astype(str)
).pivot(index='ID', columns='email_number', values='email').reset_index()

# Merge the other columns from main_df - this is why we needed separate dataframes
main_df = pd.merge(main_df, emails_df, on='ID', how='left').drop('email', axis=1)


# %%
## Part 2 - Split the category column to multiple category_ columns

# Clean the outlet_topic column. We only allow alphanumeric, parenthesis, whitespace, comma, and slash
category_df['category'] = category_df['category'].apply(lambda x: re.sub(r"[^\w\s,()/]", "", x))

# Split the cleaned string on ','
category_df['category'] = category_df['category'].str.split(',')

# Check if the resulting list contains only an empty string. If it does, replace it with an empty list.
category_df['category'] = category_df['category'].apply(lambda x: [] if len(x) == 1 and x[0] == '' else x)

# Explode, generate topic number, and pivot directly
category_df = category_df.explode('category').assign(
    category_number=lambda x: (x.groupby('ID').cumcount() + 1).astype(str).radd('category_')
).pivot(index='ID', columns='category_number', values='category').reset_index()

# Merge back the other columns from main_df
main_df = pd.merge(main_df, category_df, on='ID', how='left').drop('category', axis=1)

# %%
## Part 3 - Checking the URL Structures from the website column

# Extract the protocol from URLs using str.extract(). Use this regex to capture all values in front of the last "//"
main_df['protocol'] = main_df['website'].str.extract(
                            r'^(.*)(?=//)'
                            )

# Replace empty strings with np.nan to indicate missing values
main_df['protocol'].replace('', np.nan, inplace=True)

# Urlparse to extract the domain extension and create custom function called extract_url_info
def extract_url_info(url):
    if pd.isna(url):
        return pd.Series([np.nan], index=['domainExtension'])
    
    parsed_url = urlparse(str(url))
    domain = parsed_url.netloc or parsed_url.path  # Extracts "sub.google.com", "google.co.uk" etc.
    
    domain_parts = domain.split('.')
    
    if len(domain_parts) > 2 and len(domain_parts[-2]) == 2:
        # This is likely a ccTLD like "co.uk"
        domainExtension = '.'.join(domain_parts[-2:])
    elif len(domain_parts) > 1:
        # This is a more generic TLD like "com"
        domainExtension = domain_parts[-1]
    else:
        domainExtension = np.nan  # Sets to np.nan if there's no domain extension
    
    return pd.Series([domainExtension], index=['domainExtension'])


# Apply the extract_url_info function to "domainExtension" column
main_df['domainExtension'] = main_df['website'].apply(extract_url_info)['domainExtension']

#now we have protocol column, let's check all unique protocols
test_unique_values_protocol = main_df['protocol'].unique()

#now we have domainExtension column, let's check all unique domain extensions
test_unique_values_domainExtension = main_df['domainExtension'].unique()

# %%
## Part 4a - Data Cleaning for the website column

# Now we know the other protocols which are not "https://", let's fix them
main_df['website'] = main_df['website'].str.replace(
                            'http://', 'https://'
                            )

main_df['website'] = main_df['website'].str.replace(
                            'hhttp://', 'https://'
                            )

# Let's check again after we make the latest changes
main_df['protocol'] = main_df['website'].str.extract(
                            r'^(.*)(?=//)'
                            )

main_df['protocol'].replace('', np.nan, inplace=True)

test_unique_values_protocol_2 = main_df['protocol'].unique()

# There's one more invalid protocol to be fixed based on test_unique_values_protocol_2. Let's fix that
main_df['website'] = main_df['website'].str.replace(
                            'hhttps://', 'https://'
                            )

# Let's check again after we make the latest changes
main_df['protocol'] = main_df['website'].str.extract(
                            r'^(.*)(?=//)'
                            )

main_df['protocol'].replace('', np.nan, inplace=True)

test_unique_values_protocol_3 = main_df['protocol'].unique()

# We are good now!

# %%
## Part 4b - Still Data Cleaning for the website column, let's fix all URLs that do not have any protocol

# Check if the first characters of start with 'https://' by using str.startswith and assign it to a boolean mask
mask = main_df['website'].str.startswith('https://', na=False)

# Add 'https://' to the beginning of strings that don't start with 'https://' using loc and the previous boolean mask
# This should change (for example) "www.google.com" to "https://www.google.com"
main_df.loc[~mask, 'website'] = 'https://' + main_df['website']

# %%
## Finalization

# Let's create main_df_2 after fixing URLs in the website column
main_df_2 = (main_df
                  .drop(columns=['protocol', 'domainExtension']))

# Let's export the dataframe to csv format
main_df_2.to_csv('completed_sheet.csv', index=False)