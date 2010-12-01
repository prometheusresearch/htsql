#
# Copyright (c) 2006-2010, Prometheus Research, LLC
# Authors: Clark C. Evans <cce@clarkevans.com>,
#          Kirill Simonov <xi@resolvent.net>
#


"""
:mod:`htsql.tr.signature`
=========================
"""


from ..util import maybe, listof


class Parameter(object):

    def __init__(self, name, is_mandatory=True, is_list=False):
        assert isinstance(name, str) and len(name) > 0
        assert isinstance(is_mandatory, bool)
        assert isinstance(is_list, bool)
        self.name = name
        self.is_mandatory = is_mandatory
        self.is_list = is_list


class Signature(object):

    parameters = []

    @classmethod
    def verify(cls, kind, arguments):
        assert isinstance(kind, type)
        assert isinstance(arguments, dict)
        assert set(arguments) == set(parameter.name
                                     for parameter in cls.parameters)
        for parameter in cls.parameters:
            value = arguments[parameter.name]
            if not parameter.is_list:
                assert isinstance(value, maybe(kind))
                if parameter.is_mandatory:
                    assert value is not None
            else:
                assert isinstance(value, listof(kind))
                if parameter.is_mandatory:
                    assert len(value) > 0

    @classmethod
    def extract(cls, instance, arguments):
        assert isinstance(arguments, dict)
        for parameter in cls.parameters:
            assert not hasattr(instance, parameter.name)
            value = arguments[parameter.name]
            setattr(instance, parameter.name, value)

    @classmethod
    def apply(cls, method, arguments):
        assert isinstance(arguments, dict)
        result = {}
        for parameter in cls.parameters:
            value = arguments[parameter.name]
            if not parameter.is_list:
                if value is not None:
                    value = method(value)
            else:
                value = [method(item) for item in value]
            result[parameter.name] = value
        return result

    @classmethod
    def iterate(cls, arguments):
        for parameter in cls.parameters:
            value = arguments[parameter.name]
            if not parameter.is_list:
                if value is not None:
                    yield value
            else:
                for item in value:
                    yield item

    @classmethod
    def freeze(cls, arguments):
        result = []
        for parameter in cls.parameters:
            value = arguments[parameter.name]
            if parameter.is_list:
                value = tuple(value)
            result.append(value)
        return tuple(result)


