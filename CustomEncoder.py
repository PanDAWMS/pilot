import ATLASSiteInformation
import Configuration
import Job
import Node
import Site
import SiteInformation

try:
    import simplejson as json
except ImportError:
    import json


TYPES = {'Job': Job.Job,
         'Node': Node.Node,
         'Site': Site.Site,
         'SiteInformation': SiteInformation.SiteInformation,
         'ATLASSiteInformation': ATLASSiteInformation.ATLASSiteInformation}


class CustomTypeEncoder(json.JSONEncoder):
    """A custom JSONEncoder class that knows how to encode core custom objects.

    Custom objects are encoded as JSON object literals (ie, dicts) with
    one key, '__TypeName__' where 'TypeName' is the actual name of the
    type to which the object belongs.  That single key maps to another
    object literal which is just the __dict__ of the object encoded.
    """

    def default(self, obj):
        """Encode an object from the types supported."""
        if isinstance(obj, tuple(TYPES.values())):
            key = '__%s__' % obj.__class__.__name__
            return {key: obj.__dict__}
        return json.JSONEncoder.default(self, obj)


def CustomTypeDecoder(dct):
    """Return an object from the types supported."""
    if len(dct) == 1:
        type_name, _ = dct.items()[0]
        type_name_stripped = type_name.strip('_')
        if type_name_stripped in TYPES:
            obj = TYPES[type_name_stripped]()
            obj.__dict__ = dct[type_name]
            return obj
    return dct


class ConfigurationSerializer(object):
    """Encoder for the Configuration objects."""

    @classmethod
    def serialize(cls, obj):
        """Default encoder for the configuration.

        Basically, it encodes the dictionary of configuration values
        in a JSON or pickle file. It is necessary that every value
        is serializable.

        Arguments:
          obj: a Configuration object, if not, calls the default encoder.
        Returns:
          the str with the encoded Configuration object.
        """
        return json.dumps(obj, cls=CustomTypeEncoder)

    @classmethod
    def deserialize(cls, serial_obj):
        """Default de-serializer for the configuration.

        Decodes the dictionary of configurations and returns the object. It
        will be a new object, so singleton as expected.

        Arguments:
          serial_obj: JSON string to deserialize.
        Returns:
          the Configuration object loaded.
        """
        dct = json.loads(serial_obj, object_hook=CustomTypeDecoder)
        env = Configuration.Configuration()
        env.update(dct)
        return env

    @classmethod
    def serialize_file(cls, obj, file_path='./data.json'):
        """
        Default encoder to a file for the configuration.
        """
        file_stream = open(file_path, 'wb')
        json.dump(obj, file_stream, cls=CustomTypeEncoder)
        file_stream.flush()
        file_stream.close()

    @classmethod
    def deserialize_file(cls, file_path='./data.json'):
        """
        Default de-serializer from a file for the configuration.
        """
        file_stream = open(file_path, 'rb')
        dct = json.load(file_stream, object_hook=CustomTypeDecoder)
        file_stream.close()
        env = Configuration.Configuration()
        env.update(dct)
        return env
