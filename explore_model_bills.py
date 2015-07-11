
# coding: utf-8

# ### The American Legislative Exchange Council ([ALEC](http://www.alec.org/)) is a proxy for corporations to advance their interests by influencing policies at the level of U.S. state legislatures.
#  
# #### The influence of ALEC has been discussed in a humorous but alarming [segment](https://www.youtube.com/watch?v=aIMgfBZrrZ8) of the show "Last Week Tonight with John Oliver" in November 2014.  For a more in depth look at ALEC, see [ALEC Exposed](http://www.alecexposed.org/wiki/ALEC_Exposed).  
#  
# #### In this notebook, we'll scrape the ["model" bills](http://www.alec.org/model-legislation/) on ALEC's website using **Spark**.

# In[1]:

## Get the urls of each ALEC model bill using the libraries bs4 (BeautifulSoup), requests (for parsing the html document), re (regular expressions for filtering)

import requests
from bs4 import BeautifulSoup
from bs4 import SoupStrainer
import re


# In[2]:

## parse ALEC page containing links to all model policies
models_main = requests.get("http://www.alec.org/model-legislation/")

## create BeautifulSoup object from the ALEC html page
soupm = BeautifulSoup(models_main.text, "html.parser")


# In[3]:

## function to filter in find_all()   
def model_legislation(href):
    """ search for keywords in href to filter 
    Args:
        href (str): href value returned by find_all()
    Returns:
        bool: T if href is a non-empty string containing the pattern
    """
    return href and re.compile("model-legislation/").search(href)


# In[4]:

## get all model legislation links
soupm_filtered = soupm.find_all("a", href = model_legislation)

## extract model legistlation URLs from 'a' tags
links_list = [link.get('href') for link in soupm_filtered]
links_set = set(links_list)

## how many URLs are there?
print(len(links_set))


# #### The set of model bill links are stored in `links_set`.  Next we define `scrape_bill()` to scrape the full texts of the bills (one bill per link in `links_set`) and `get_taskforces()` to get the task force labels for the bills (if existing).

# In[5]:

## function to retrieve main text of bill
def scrape_bill(href):
    """ Scrape the text from the bill
    Args:
        href (str): url to model bill
    Returns:
        a list of - text (str): the scraped text (without xml tags); 
                    taskf_terms (list): list of 0 or more strings
    """
    try:
        r = requests.get(href)
        only_tags_with_id_main = SoupStrainer(id="main")
        mainsoup = BeautifulSoup(r.text, "lxml", parse_only = only_tags_with_id_main)        
        text = mainsoup.get_text(' ', strip=True)
        
        ## extract task force label(s) at bottom of bill
        found_taskf_terms = mainsoup.find_all(id = "related-task-forces")
        if found_taskf_terms:
            taskf_terms = (found_taskf_terms[0]
                           .get_text('|', strip = True)
                           .split('|'))
            
            return [text, taskf_terms]
        else:
            return [text, []]
    except:
        return []


# #### Create a base RDD of the list (actually set) of links so we can parallelize the scraping

# In[6]:

linksRDD = sc.parallelize(links_set, 8)
print(type(linksRDD))


# #### Scrape the full text of the bills and their task force label(s).  
# 
# Create a pair RDD, `billsRDD`, that has elements `(k, v)`, where the `k` is the URL of a bill, and `v` is a list containing `["scraped contents of bill", ["task force label 1", "task force label 2", ...]]`.

# In[7]:

billsRDD = linksRDD.map(lambda x: (x, scrape_bill(x))).cache()


# In[8]:

## How many bills were successfully scraped?
scraped_billsRDD = billsRDD.filter(lambda (bill_URL, v): v)

print('{0} bills were successfully scraped of {1} URLs.'.format(scraped_billsRDD.count(), len(links_set)))

## How many bills were successfully scraped and had task force labels?
labeled_billsRDD = scraped_billsRDD.filter(lambda (bill_URL, v): v[1]).cache()

print('{0} scraped bills have task force labels.'.format(labeled_billsRDD.count()))


# #### We'll save analysis of the main contents of the bills for a later time.  For now, let's take a look at the distribution of task forces/categories among the labeled bills.
# 
# Create a pair RDD, `bills_taskforceRDD`, containing tuples of bill URLs and task force labels.

# In[9]:

bills_taskforceRDD = (labeled_billsRDD
                      .mapValues(lambda v: v[1])
                      .cache())

## How many bills have at least one task force category?
print('{0} bills have at least one task force label'.format(bills_taskforceRDD.count()))

## How many bills fall under more than one task force category?
print('{0} bills belong to more than 1 task force'.format(bills_taskforceRDD.filter(lambda (b, v): len(v)>1).count()))


# In[10]:

## Take a look at a few of the task force labels
print(bills_taskforceRDD.take(3))


# #### Next, let's determine how many bills fall into each task force/category.  Note, some bills fall into multiple categories, e.g. with tuples (bill A, [category 1, category 2, ...])

# In[11]:

## Define count_tuple, a helper function that maps each task force label of a bill to a key-value pair with value 1 to facilitate counting.
def count_tuple(bill):
    """ Map a tuple of a bill_URL and split it into 1 or more (the same number as the number of task forces) tuples
        (bill A, [category 1, category 2, ...]) -> [(category 1, 1), (category 2, 1)]
    Args:
        bill (tuple): (k, v): k is the bill_URL, v is a list of strings (task force categories) of length 1 or more
    Returns:
        a list of tuples 
    """
    categories = bill[1]
    return [(task_force, 1) for task_force in categories]

bills_per_taskfRDD = (bills_taskforceRDD
                      .flatMap(count_tuple)
                      .reduceByKey(lambda a, b: a + b)
                      .cache())


# In[12]:

## How many (distinct) task force categories are there?
print("There are {0} task force categories".format(bills_per_taskfRDD.count()))


# In[13]:

## View all the categories with their associated counts
bills_per_taskfRDD.sortBy(lambda x: -x[1]).collect()


# In[14]:

from pyspark.sql import Row
# Create a DataFrame and visualize using show()
taskforceRow = bills_per_taskfRDD.map(lambda (x, y): Row(task_force=x, num_bills=y))
taskforceDF = sqlContext.createDataFrame(taskforceRow)
taskforceDF.show()

