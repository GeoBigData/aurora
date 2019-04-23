# aurora
This Python package contains utilities for inventorying and assessing AWS costs for AWS S3, EC2, and EBS resources.

------------
### Development
#### Requirements:
- Anaconda or Miniconda

#### To set up your local development environment:
This will install the aurora package from the local repo in editable mode. Any changes to Python files within the local repo should immediately take effect in this environment.

1. Clone the repo
`git clone https://github.com/GeoBigData/aurora.git`

2. Move into the local repo
`cd aurora`

3. Create conda virtual environment
`conda env create -f environment.yml`

4. Activate the environment
`source activate aurora`

5. Install floodwatch Python package
`pip install -r requirements_dev.txt`
