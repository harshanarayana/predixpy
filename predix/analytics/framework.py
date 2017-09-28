import os
import json
import uuid
import logging
import requests
import furl

# Apply Python2 To 3 fixes for handling URL Parse.
from future.standard_library import install_aliases

import predix.config
import predix.service
import predix.security.uaa

# Apply Python2 To 3 fixes for handling URL Parse.
install_aliases()
from urllib.parse import urljoin


class Framework(object):
    """
    A Client Library for working with the Predix Analytics Framework.
    For more details on the Available Services and feature please visit
    https://www.predix.io/services/service.html?id=2114
    """
    def __init__(self, *args, **kwargs):
        super(Framework, self).__init__(*args, **kwargs)

        zone_id_key = predix.config.get_env_key(self, 'zone_id')
        self.zone_id = os.getenv(zone_id_key)
        if self.zone_id is None:
            raise ValueError("%s environment unset" % zone_id_key)

        base_uri_key = predix.config.get_env_key(self, 'uri')
        self.base_uri = os.getenv(base_uri_key)
        if self.base_uri is None:
            raise ValueError("%s environment unset" % base_uri_key)

        self.service = predix.service.Service(self.zone_id)


class Catalog(Framework):
    """
    A Client Library that Provides the Features of all the Catalog API
    that is available under the Analytics Framework.
    """
    def __init__(self, catalog_api='/api/v1/catalog', catalog_type='analytics',
                 *args, **kwargs):
        super(Catalog, self).__init__(*args, **kwargs)

        self.catalog_type = catalog_type

        if catalog_api is None:
            raise ValueError("api_default cannot be None")

        self.uri = urljoin(self.base_uri, catalog_api)
        self.furl = furl(self.uri)
        self.furl.path.add(catalog_type)

    @predix.config.valid_for("catalog_type", "analytics")
    def get_analytics(self, page=None, size=25, sort_order="asc",
                      sortable_fields="name", taxonomy_path=None):
        """
        Returns all analytic catalog entries as specified by page
        and sort criteria.
        """
        furl_url = self.furl
        furl_url.query.params = {
            "size": size,
            "sortOrder": sort_order,
            "sortableFields": sortable_fields,
        }
        if page is not None:
            furl_url.query.params.update({
                "page": page
            })

        if taxonomy_path is not None:
            furl_url.query.params.update({
                "taxonomyPath": taxonomy_path
            })
        return self.service._get(furl_url.url)

    @predix.config.valid_for("catalog_type", "analytics")
    def get_analytics_version(self, analytics_name):
        """
        Returns all versions of the analytic catalog entry with the
        given name.
        """
        furl_url = self.furl
        furl_url.query.params = {
            "name": analytics_name
        }
        return self.service._get(furl_url.url)

    @predix.config.valid_for("catalog_type", "analytics")
    def get_analytics_for_id(self, analytics_id):
        furl_url = self.furl
        furl_url.path.add(analytics_id)
        return self.service._get(furl_url.url)

    @predix.config.valid_for("catalog_type", "analytics")
    def get_artifact_for_analytics_id(self, analytics_id):
        furl_url = self.furl
        furl_url.path.add(analytics_id).add("artifacts")
        return self.service._get(furl_url.url)

    @predix.config.valid_for("catalog_type", "analytics")
    def get_deployment_status_for_analytics_id(self, analytics_id, request_id):
        furl_url = self.furl
        furl_url.path.add(analytics_id).add("deployment").add(request_id)
        return self.service._get(furl_url.url)

    @predix.config.valid_for("catalog_type", "analytics")
    def get_logs_for_analytics_id(self, analytics_id):
        furl_url = self.furl
        furl_url.path.add(analytics_id).add("logs")
        return self.service._get(furl_url.url, convert_to_json=False)

    @predix.config.valid_for("catalog_type", "analytics")
    def get_validation_status_for_analytics_id(
            self, analytics_id, validation_request_id):
        furl_url = self.furl
        furl_url.path.add(analytics_id).add(
            "validation").add(validation_request_id)
        return self.service._get(furl_url.url)

    def post_analytics(self, analytics_body):
        if type(analytics_body) is not dict:
            raise TypeError("Analytics Body should be a Dictionary")
        return self.service._post(self.furl.url, analytics_body)

    def post_analytics_deployment(self, analytics_id, deployment_body={}):
        if type(deployment_body) is not dict:
            raise ValueError(
                "Analytics Deployment Body should be a Dictionary")
        furl_url = self.furl
        furl_url.path.add(analytics_id).add("deployment")
        return self.service._post(furl_url.url, deployment_body)

    def post_analytics_execution(self, analytics_id, input_id=None, execution_body=None):
        if execution_body is None:
            raise ValueError("Analytics Execution Body needs a valid Value.")
        furl_url = self.furl
        furl_url.path.add(analytics_id).add("execution")
        if input_id is not None:
            furl_url.query.params = {
                "inputId": input_id
            }
        return self.service._post(furl_url.url, execution_body)

    def post_analytics_validation(self, analytics_id, input_id=None, validation_body=None):
        if validation_body is None:
            raise ValueError("Analytics Validation Body needs a value.")
        furl_url = self.furl
        furl.path.add(analytics_id).add("validation")
        if input_id is not None:
            furl_url.path.params = {
                "inputId": input_id
            }
        return self.service._post(furl_url.url, validation_body)

    def update_analytics_for_id(self, analytics_id, analytics_body):
        if type(analytics_body) is not dict:
            raise TypeError("Analytics Body should be of type Dictionary")
        furl_url = self.furl
        furl_url.path.add(analytics_id)
        return self.service._put(furl_url.url, analytics_body)
