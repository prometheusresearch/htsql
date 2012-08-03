#
# Copyright (c) 2006-2012, Prometheus Research, LLC
#


from . import wsgi
from ...core.addon import Addon, Parameter
from ...core.validator import BoolVal


class TweakCSRFAddon(Addon):

    name = 'tweak.csrf'
    hint = """Cross-Site Request Forgery (CSRF) protection"""
    help = """
    This addon provides protection against cross-site request
    forgery (CSRF) attacks.

    A CSRF attack tricks the user to visit the attacker's website,
    which then submits database queries to the HTSQL service from
    the user's account.  Even though the browser would not permit
    the malicious website to read the output of the queries, this
    form of attack can be used for denial of service or changing
    the data in the database.  For background on CSRF, see
    http://en.wikipedia.org/wiki/Cross-site_request_forgery.

    This addon requires all HTSQL requests to submit a secret
    token in two forms:

    * as a cookie `htsql-csrf-token`;
    * as HTTP header `X-HTSQL-CSRF-Token`.

    If the token is not submitted, the addon prevents the request
    from reading or updating any data in the database.

    If the `allow_cs_read` parameter is set, a request is permitted
    to read data from the database even when the secret token is
    not provided.

    If the `allow_cs_write` parameter is set, a request is permitted
    to update data in the database even if the secret token is
    not provided.
    """

    parameters = [
            Parameter('allow_cs_read', BoolVal(), default=False,
                      hint="""permit cross-site read requests"""),
            Parameter('allow_cs_write', BoolVal(), default=False,
                      hint="""permit cross-site write requests"""),
    ]


