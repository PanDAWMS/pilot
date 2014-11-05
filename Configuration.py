#!/usr/bin/evn python

"""Configuration manager to store key/value variables.

This is a configuration manager that offers a dictionary-like way to store
configuration values, in-depth serializable and thread and multi-process safe.
"""

import threading
import unittest


class Singleton(type):
    """Metaclass to convert any other new-style class in a singleton."""
    __instances = {}
    __lookup_lock = threading.Lock()
    __write_lock = threading.Lock()

    def __call__(cls, *args, **kwargs):
        """Stores a new instance of the class on class load.

        When the class is loaded, it stores a new instance in the dictionary
        that tracks the class list. On every other load, it returns the already
        existing instace for each class. The safe-threaded implementation is
        achieved by a double-checking implementation along with a class lock.
        This is thread-safe in the majority of cases, except in the case of an
        out-of-order write performed by the Python interpreter (unlikely), to
        solve this, a second lock is used.

        Arguments:
          cls: class object being instantiated.
          args, kwargs: any argument for the class instantiation.
        Returns:
          the class object for cls.
        """
        if cls not in cls.__instances:
            cls.__lookup_lock.acquire()
            if cls not in cls.__instances:
                cls.__write_lock.acquire()
                cls.__instances[cls] = (super(Singleton, cls)
                                        .__call__(*args, **kwargs))
                cls.__write_lock.release()
            cls.__lookup_lock.release()
        return cls.__instances[cls]


class Configuration(dict):
    """Stores configuration values in a dictionary-alike way.

    Apart from the dictionary lookup for keys, is able to perform looks by
    attribute. The backend is the dictionary itself.
    """
    __metaclass__ = Singleton

    def __init__(self, *args, **kwargs):
        """Delegate the initialization to the dict __init__."""
        super(Configuration, self).__init__(*args, **kwargs)

    def __setattr__(self, name, value):
        """Stores the new attribute in the dict backend:

        Arguments:
          name: str that identifies the attribute, will be its key.
          value: value of the attribute, must be JSON serializable.
        """
        self[name] = value

    def __getattr__(self, name):
        """Retrieves the attribute from the dict backend.

        Arguments:
          name: str that identifies the attribute, will be its key.
        Returns:
          the attribute value, raises AttributeError if not found.
        """
        try:
            return self[name]
        except KeyError:
            raise AttributeError('%s has no attribute %s' %
                                 (self.__class__.__name__, name))

    def __unicode__(self):
        """Description of this object."""
        return "%s at %d with %d values" % (self.__class__.__name__,
                                            id(self), len(self))

    def __str__(self):
        """Sub for legacy compatibilty."""
        return unicode(self).encode('utf-8')


class ConfigurationTest(unittest.TestCase):
    """Unit tests for the configuration class."""

    def setUp(self):
        self.random_value = 1113
        self.configuration = Configuration()
        self.expected_json = '{"a": 1, "b": "b"}'

    def test_creation(self):
        """Test whether object creation always yields the same object."""
        self.assertEqual(id(self.configuration), id(Configuration()),
                         'Object creation did not yield a singleton')

    def test_dict_attr(self):
        """Test the setting of an attribute."""
        self.configuration['test_dict'] = self.random_value
        self.assertEqual(self.random_value, self.configuration.test_dict,
                         'Getting and attr key did not yield current value')

    def test_attr_dict(self):
        """Test the getting of an attribute."""
        self.configuration.test_attr = self.random_value
        self.assertEqual(self.random_value, self.configuration['test_attr'],
                         'Getting a dict key did not yield current value')


if __name__ == '__main__':
    unittest.main()
