from setuptools import setup

setup(
    name='hydroview-flask',
    packages=['app'],
    include_package_data=True,
    install_requires=[
        'amqp==2.1.4',
        'aniso8601==1.2.0',
        'billiard==3.5.0.2',
        'cassandra-driver==3.7.1',
        'celery==5.2.2',
        'click==6.6',
        'flask',
        'itsdangerous',
        'Jinja2==2.8',
        'kombu==4.0.2',
        'MarkupSafe==0.23',
        'python-dateutil==2.6.0',
        'pytz==2016.10',
        'PyYAML==3.12',
        'six==1.10.0',
        'vine==1.1.3',
        'Werkzeug==0.11.11'
    ],
)
