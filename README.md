# NewsCrawler

## Architecture
![jietu20180805-125721 2x](https://user-images.githubusercontent.com/10342877/43688006-1e6dde4a-98af-11e8-9a5c-2c7173357618.jpg)


## Source
1. News API: https://newsapi.org/
2. Collect News from New York times.
3. Message Queue: https://www.cloudamqp.com

mkvirtualenv --no-site-packages goose
git clone https://github.com/grangier/python-goose.git
cd python-goose
pip install -r requirements.txt
python setup.py install