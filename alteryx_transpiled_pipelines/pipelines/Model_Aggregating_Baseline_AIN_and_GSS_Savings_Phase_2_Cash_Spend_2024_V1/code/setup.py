from setuptools import setup, find_packages
setup(
    name = 'Model_Aggregating_Baseline_AIN_and_GSS_Savings_Phase_2_Cash_Spend_2024_V1',
    version = '1.0',
    packages = find_packages(include = ('job*', )) + ['prophecy_config_instances'],
    package_dir = {'prophecy_config_instances' : 'configs/resources/config'},
    package_data = {'prophecy_config_instances' : ['*.json', '*.py', '*.conf']},
    description = 'workflow',
    install_requires = [
'prophecy-libs==1.9.29'],
    entry_points = {
'console_scripts' : [
'main = job.pipeline:main'], },
    data_files = [(".prophecy", [".prophecy/workflow.latest.json"])],
    extras_require = {
'test' : ['pytest', 'pytest-html'], }
)
