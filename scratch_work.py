# table that contains "effective dates, model pickle object, json of the pip freeze output"
# after model run...
# create pip freeze json
# create model pickle object 
# grab input effective dates

import json
import pkg_resources

installed_packages = [dist.project_name for dist in pkg_resources.working_set]
installed_packages_dict = {pkg: pkg_resources.get_distribution(pkg).version for pkg in installed_packages}

print(json.dumps(installed_packages_dict, indent=4))