"""

Methods from charmhelpers lib.

Used to replace some of the charmhelper commands such as open/close ports
and render methods.

"""

import io
import re
import os
import pwd
import grp
import json
import logging
import ipaddress
import subprocess

logger = logging.getLogger(__name__)

__all__ = [
    "open_port",
    "close_port",
    "open_ports",
    "close_ports",
    "get_hostname",
    "render",
    "render_from_string",
    "apt_update",
    "apt_install",
    "mount",
    "umount",
    "add_source",
    "GPGKeyError",
    "get_address_in_network"
]


def ns_query(address):
    try:
        import dns.resolver
    except ImportError:
        apt_install('python3-dnspython', fatal=True)
        import dns.resolver

    if isinstance(address, dns.name.Name):
        rtype = 'PTR'
    elif isinstance(address, str):
        rtype = 'A'
    else:
        return None

    try:
        answers = dns.resolver.query(address, rtype)
    except dns.resolver.NXDOMAIN:
        return None

    if answers:
        return str(answers[0])
    return None

def is_ip(address):
    """
    Returns True if address is a valid IP address.
    """
    try:
        # Test to see if already an IPv4/IPv6 address
        address = ipaddress.ip_address(address)
        return True
    except (ValueError):
        return False


def get_hostname(address, fqdn=True):
    """
    Resolves hostname for given IP, or returns the input
    if it is already a hostname.
    """
    if is_ip(address):
        try:
            import dns.reversename
        except ImportError:
            subprocess.check_output(["apt", "install", "-y", "python3-dnspython"])
            import dns.reversename

        rev = dns.reversename.from_address(address)
        result = ns_query(rev)

        if not result:
            try:
                result = socket.gethostbyaddr(address)[0]
            except Exception:
                return None
    else:
        result = address

    if fqdn:
        # strip trailing .
        if result.endswith('.'):
            return result[:-1]
        else:
            return result
    else:
        return result.split('.')[0]


def _port_op(op_name, port, protocol="TCP"):
    """Open or close a service network port"""
    _args = [op_name]
    icmp = protocol.upper() == "ICMP"
    if icmp:
        _args.append(protocol)
    else:
        _args.append('{}/{}'.format(port, protocol))
    try:
        subprocess.check_call(_args)
    except subprocess.CalledProcessError:
        # Older Juju pre 2.3 doesn't support ICMP
        # so treat it as a no-op if it fails.
        if not icmp:
            raise


def open_port(port, protocol="TCP"):
    """Open a service network port"""
    _port_op('open-port', port, protocol)


def close_port(port, protocol="TCP"):
    """Close a service network port"""
    _port_op('close-port', port, protocol)


def open_ports(start, end, protocol="TCP"):
    """Opens a range of service network ports"""
    _args = ['open-port']
    _args.append('{}-{}/{}'.format(start, end, protocol))
    subprocess.check_call(_args)


def close_ports(start, end, protocol="TCP"):
    """Close a range of service network ports"""
    _args = ['close-port']
    _args.append('{}-{}/{}'.format(start, end, protocol))
    subprocess.check_call(_args)


def _charmhelper_mkdir(path, owner='root', group='root', perms=0o555, force=False):
    """Create a directory"""
    logger.info("Making dir {} {}:{} {:o}".format(path, owner, group,
                                        perms))
    uid = pwd.getpwnam(owner).pw_uid
    gid = grp.getgrnam(group).gr_gid
    realpath = os.path.abspath(path)
    path_exists = os.path.exists(realpath)
    if path_exists and force:
        if not os.path.isdir(realpath):
            logger.info("Removing non-directory file {} prior to mkdir()".format(path))
            os.unlink(realpath)
            os.makedirs(realpath, perms)
    elif not path_exists:
        os.makedirs(realpath, perms)
    os.chown(realpath, uid, gid)
    os.chmod(realpath, perms)


def _charmhelper_write_file(path, content, owner='root', group='root', perms=0o444):
    """Create or overwrite a file with the contents of a byte string."""
    uid = pwd.getpwnam(owner).pw_uid
    gid = grp.getgrnam(group).gr_gid
    # lets see if we can grab the file and compare the context, to avoid doing
    # a write.
    existing_content = None
    existing_uid, existing_gid, existing_perms = None, None, None
    try:
        with open(path, 'rb') as target:
            existing_content = target.read()
        stat = os.stat(path)
        existing_uid, existing_gid, existing_perms = (
            stat.st_uid, stat.st_gid, stat.st_mode
        )
    except Exception:
        pass
    if content != existing_content:
        logger.debug("Writing file {} {}:{} {:o}".format(path, owner, group, perms))
        with open(path, 'wb') as target:
            os.fchown(target.fileno(), uid, gid)
            os.fchmod(target.fileno(), perms)
            if isinstance(content, str):
                content = content.encode('UTF-8')
            target.write(content)
        return
    # the contents were the same, but we might still need to change the
    # ownership or permissions.
    if existing_uid != uid:
        logger.debug("Changing uid on already existing content: {} -> {}"
            .format(existing_uid, uid))
        os.chown(path, uid, -1)
    if existing_gid != gid:
        logger.debug("Changing gid on already existing content: {} -> {}"
            .format(existing_gid, gid))
        os.chown(path, -1, gid)
    if existing_perms != perms:
        logger.debug("Changing permissions on existing content: {} -> {}"
            .format(existing_perms, perms))
        os.chmod(path, perms)


def render(source, target, context, owner='root', group='root',
           perms=0o444, templates_dir=None, encoding='UTF-8',
           template_loader=None, config_template=None):
    """
    Render a template.
    The `source` path, if not absolute, is relative to the `templates_dir`.
    The `target` path should be absolute.  It can also be `None`, in which
    case no file will be written.
    The context should be a dict containing the values to be replaced in the
    template.
    config_template may be provided to render from a provided template instead
    of loading from a file.
    The `owner`, `group`, and `perms` options will be passed to `write_file`.
    If omitted, `templates_dir` defaults to the `templates` folder in the charm.
    The rendered template will be written to the file as well as being returned
    as a string.
    Note: Using this requires python3-jinja2; if it is not installed, calling
    this will attempt to use charmhelpers.fetch.apt_install to install it.
    """

    from jinja2 import FileSystemLoader, Environment, exceptions

    if template_loader:
        template_env = Environment(loader=template_loader)
    else:
        if templates_dir is None:
            templates_dir = os.path.join(os.environ.get('CHARM_DIR', ''), 'templates')
        template_env = Environment(loader=FileSystemLoader(templates_dir))

    # load from a string if provided explicitly
    if config_template is not None:
        template = template_env.from_string(config_template)
    else:
        try:
            source = source
            template = template_env.get_template(source)
        except exceptions.TemplateNotFound as e:
            raise e
    content = template.render(context)
    if target is not None:
        target_dir = os.path.dirname(target)
        if not os.path.exists(target_dir):
            # This is a terrible default directory permission, as the file
            # or its siblings will often contain secrets.
            _charmhelper_mkdir(os.path.dirname(target), owner, group, perms=0o755)
        _charmhelper_write_file(target, content.encode(encoding), owner, group, perms)
    return content


def render_from_string(source, target, context, owner='root', group='root',
                       perms=0o444, templates_dir=None, encoding='UTF-8',
                       template_loader=None, config_template=None):
    """
    Render a template.
    The `source` is the string to be rendered.
    The `target` path should be absolute.  It can also be `None`, in which
    case no file will be written.
    The context should be a dict containing the values to be replaced in the
    template.
    config_template may be provided to render from a provided template instead
    of loading from a file.
    The `owner`, `group`, and `perms` options will be passed to `write_file`.
    If omitted, `templates_dir` defaults to the `templates` folder in the charm.
    The rendered template will be written to the file as well as being returned
    as a string.
    Note: Using this requires python3-jinja2; if it is not installed, calling
    this will attempt to use charmhelpers.fetch.apt_install to install it.
    """

    from jinja2 import FileSystemLoader, Environment, exceptions

    if template_loader:
        template_env = Environment(loader=template_loader)
    else:
        if templates_dir is None:
            templates_dir = os.path.join(os.environ.get('CHARM_DIR', ''), 'templates')
        template_env = Environment(loader=FileSystemLoader(templates_dir))

    # load from a string if provided explicitly
    if config_template is not None:
        template = template_env.from_string(config_template)
    else:
        try:
            source = source
            template = template_env.from_string(source)
        except exceptions.TemplateNotFound as e:
            raise e
    content = template.render(context)
    if target is not None:
        target_dir = os.path.dirname(target)
        if not os.path.exists(target_dir):
            # This is a terrible default directory permission, as the file
            # or its siblings will often contain secrets.
            _charmhelper_mkdir(os.path.dirname(target), owner, group, perms=0o755)
        _charmhelper_write_file(target, content.encode(encoding), owner, group, perms)
    return content


def apt_update(fatal=False):
    """Update local apt cache."""
    cmd = ['apt-get', 'update']
    return subprocess.check_output(cmd)


def apt_install(packages, options=None, fatal=False, quiet=False):
    """Install one or more packages.
    :param packages: Package(s) to install
    :type packages: Option[str, List[str]]
    :param options: Options to pass on to apt-get
    :type options: Option[None, List[str]]
    :param fatal: Whether the command's output should be checked and
                  retried.
    :type fatal: bool
    :param quiet: if True (default), suppress log message to stdout/stderr
    :type quiet: bool
    :raises: subprocess.CalledProcessError
    """
    if options is None:
        options = ['--option=Dpkg::Options::=--force-confold']

    cmd = ['apt-get', '--assume-yes']
    cmd.extend(options)
    cmd.append('install')
    if isinstance(packages, str):
        cmd.append(packages)
    else:
        cmd.extend(packages)
    if not quiet:
        logger.info("Installing {} with options: {}"
            .format(packages, options))
    return subprocess.check_output(cmd)


def add_source(source, key=None, fail_invalid=False):
    """Add a package source to this system.
    Simplified version where the only format of source accepted is:
    deb [...] <repo-url> <pockets>
    """
    m = re.match(r"^((?:deb |http:|https:|ppa:).*)$", source)
    if key:
        try:
            import_key(key)
        except GPGKeyError as e:
            raise SourceConfigError(str(e))
    else:
        raise Exception("add_source: Missing GPG key")
    _add_apt_repository(*m.groups())


class GPGKeyError(Exception):
    """Exception occurs when a GPG key cannot be fetched or used.  The message
    indicates what the problem is.
    """
    pass


def fstab_add(dev, mp, fs, options=None):
    """Adds the given device entry to the /etc/fstab file"""
    return Fstab.add(dev, mp, fs, options=options)

def fstab_remove(mp):
    """Remove the given mountpoint entry from /etc/fstab"""
    return Fstab.remove_by_mountpoint(mp)

def mount(device, mountpoint, options=None, persist=False, filesystem="ext3"):
    """Mount a filesystem at a particular mountpoint"""
    cmd_args = ['mount']
    if options is not None:
        cmd_args.extend(['-o', options])
    cmd_args.extend([device, mountpoint])
    try:
        subprocess.check_output(cmd_args)
    except subprocess.CalledProcessError as e:
        logger.info('Error mounting {} at {}\n{}'.format(device, mountpoint, e.output))
        return False

    if persist:
        return fstab_add(device, mountpoint, filesystem, options=options)
    return True

def umount(mountpoint, persist=False):
    """Unmount a filesystem"""
    cmd_args = ['umount', mountpoint]
    try:
        subprocess.check_output(cmd_args)
    except subprocess.CalledProcessError as e:
        return False

    if persist:
        return fstab_remove(mountpoint)
    return True


class Fstab(io.FileIO):
    """This class extends file in order to implement a file reader/writer
    for file `/etc/fstab`
    """

    class Entry(object):
        """Entry class represents a non-comment line on the `/etc/fstab` file
        """
        def __init__(self, device, mountpoint, filesystem,
                     options, d=0, p=0):
            self.device = device
            self.mountpoint = mountpoint
            self.filesystem = filesystem

            if not options:
                options = "defaults"

            self.options = options
            self.d = int(d)
            self.p = int(p)

        def __eq__(self, o):
            return str(self) == str(o)

        def __str__(self):
            return "{} {} {} {} {} {}".format(self.device,
                                              self.mountpoint,
                                              self.filesystem,
                                              self.options,
                                              self.d,
                                              self.p)

    DEFAULT_PATH = os.path.join(os.path.sep, 'etc', 'fstab')

    def __init__(self, path=None):
        if path:
            self._path = path
        else:
            self._path = self.DEFAULT_PATH
        super(Fstab, self).__init__(self._path, 'rb+')

    def _hydrate_entry(self, line):
        # NOTE: use split with no arguments to split on any
        #       whitespace including tabs
        return Fstab.Entry(*filter(
            lambda x: x not in ('', None),
            line.strip("\n").split()))

    @property
    def entries(self):
        self.seek(0)
        for line in self.readlines():
            line = line.decode('us-ascii')
            try:
                if line.strip() and not line.strip().startswith("#"):
                    yield self._hydrate_entry(line)
            except ValueError:
                pass

    def get_entry_by_attr(self, attr, value):
        for entry in self.entries:
            e_attr = getattr(entry, attr)
            if e_attr == value:
                return entry
        return None

    def add_entry(self, entry):
        if self.get_entry_by_attr('device', entry.device):
            return False

        self.write((str(entry) + '\n').encode('us-ascii'))
        self.truncate()
        return entry

    def remove_entry(self, entry):
        self.seek(0)

        lines = [l.decode('us-ascii') for l in self.readlines()]

        found = False
        for index, line in enumerate(lines):
            if line.strip() and not line.strip().startswith("#"):
                if self._hydrate_entry(line) == entry:
                    found = True
                    break

        if not found:
            return False

        lines.remove(line)

        self.seek(0)
        self.write(''.join(lines).encode('us-ascii'))
        self.truncate()
        return True

    @classmethod
    def remove_by_mountpoint(cls, mountpoint, path=None):
        fstab = cls(path=path)
        entry = fstab.get_entry_by_attr('mountpoint', mountpoint)
        if entry:
            return fstab.remove_entry(entry)
        return False

    @classmethod
    def add(cls, device, mountpoint, filesystem, options=None, path=None):
        return cls(path=path).add_entry(Fstab.Entry(device,
                                                    mountpoint, filesystem,
                                                    options=options))


def get_address_in_network(network):
    """Get an IPv4 or IPv6 address within the network from the host.
    :param network (str): CIDR presentation format. For example,
        '192.168.1.0/24'. Supports multiple networks as a space-delimited list.
    """
    import netifaces
    import netaddr

    def _validate_cidr(network):
        try:
            netaddr.IPNetwork(network)
        except (netaddr.core.AddrFormatError, ValueError):
            raise ValueError("Network (%s) is not in CIDR presentation format" %
                             network)

    if network is None or len(network) == 0:
        return None

    networks = network.split() or [network]
    for network in networks:
        _validate_cidr(network)
        network = netaddr.IPNetwork(network)
        for iface in netifaces.interfaces():
            try:
                addresses = netifaces.ifaddresses(iface)
            except ValueError:
                # If an instance was deleted between
                # netifaces.interfaces() run and now, its interfaces are gone
                continue
            if network.version == 4 and netifaces.AF_INET in addresses:
                for addr in addresses[netifaces.AF_INET]:
                    cidr = netaddr.IPNetwork("%s/%s" % (addr['addr'],
                                                        addr['netmask']))
                    if cidr in network:
                        return str(cidr.ip)

            if network.version == 6 and netifaces.AF_INET6 in addresses:
                for addr in addresses[netifaces.AF_INET6]:
                    cidr = _get_ipv6_network_from_address(addr)
                    if cidr and cidr in network:
                        return str(cidr.ip)

    return None


######################################
##
##  GPG INTERNAL METHODS
##
#####################################

def _add_apt_repository(spec):
    """Add the spec using add_apt_repository
    :param spec: the parameter to pass to add_apt_repository
    :type spec: str
    """
    return subprocess.check_output(['add-apt-repository', '--yes', spec])

def import_key(key):
    """Import an ASCII Armor key.
    A Radix64 format keyid is also supported for backwards
    compatibility. In this case Ubuntu keyserver will be
    queried for a key via HTTPS by its keyid. This method
    is less preferable because https proxy servers may
    require traffic decryption which is equivalent to a
    man-in-the-middle attack (a proxy server impersonates
    keyserver TLS certificates and has to be explicitly
    trusted by the system).
    :param key: A GPG key in ASCII armor format,
                  including BEGIN and END markers or a keyid.
    :type key: (bytes, str)
    :raises: GPGKeyError if the key could not be imported
    """
    key = key.strip()
    if '-' in key or '\n' in key:
        # Send everything not obviously a keyid to GPG to import, as
        # we trust its validation better than our own. eg. handling
        # comments before the key.
        logger.debug("PGP key found (looks like ASCII Armor format)")
        if ('-----BEGIN PGP PUBLIC KEY BLOCK-----' in key and
                '-----END PGP PUBLIC KEY BLOCK-----' in key):
            logger.debug("Writing provided PGP key in the binary format")
            key_bytes = key.encode('utf-8')
            key_name = _get_keyid_by_gpg_key(key_bytes)
            key_gpg = _dearmor_gpg_key(key_bytes)
            _write_apt_gpg_keyfile(key_name=key_name, key_material=key_gpg)
        else:
            raise GPGKeyError("ASCII armor markers missing from GPG key")
    else:
        logger.warning("PGP key found (looks like Radix64 format)")
        logger.warning("SECURELY importing PGP key from keyserver; "
                       "full key not provided.")
        # as of bionic add-apt-repository uses curl with an HTTPS keyserver URL
        # to retrieve GPG keys. `apt-key adv` command is deprecated as is
        # apt-key in general as noted in its manpage. See lp:1433761 for more
        # history. Instead, /etc/apt/trusted.gpg.d is used directly to drop
        # gpg
        key_asc = _get_key_by_keyid(key)
        # write the key in GPG format so that apt-key list shows it
        key_gpg = _dearmor_gpg_key(key_asc)
        _write_apt_gpg_keyfile(key_name=key, key_material=key_gpg)


def _get_keyid_by_gpg_key(key_material):
    """Get a GPG key fingerprint by GPG key material.
    Gets a GPG key fingerprint (40-digit, 160-bit) by the ASCII armor-encoded
    or binary GPG key material. Can be used, for example, to generate file
    names for keys passed via charm options.
    :param key_material: ASCII armor-encoded or binary GPG key material
    :type key_material: bytes
    :raises: GPGKeyError if invalid key material has been provided
    :returns: A GPG key fingerprint
    :rtype: str
    """
    # Use the same gpg command for both Xenial and Bionic
    cmd = 'gpg --with-colons --with-fingerprint'
    ps = subprocess.Popen(cmd.split(),
                          stdout=subprocess.PIPE,
                          stderr=subprocess.PIPE,
                          stdin=subprocess.PIPE)
    out, err = ps.communicate(input=key_material)
    out = out.decode('utf-8')
    err = err.decode('utf-8')
    if 'gpg: no valid OpenPGP data found.' in err:
        raise GPGKeyError('Invalid GPG key material provided')
    # from gnupg2 docs: fpr :: Fingerprint (fingerprint is in field 10)
    return re.search(r"^fpr:{9}([0-9A-F]{40}):$", out, re.MULTILINE).group(1)


def _get_key_by_keyid(keyid):
    """Get a key via HTTPS from the Ubuntu keyserver.
    Different key ID formats are supported by SKS keyservers (the longer ones
    are more secure, see "dead beef attack" and https://evil32.com/). Since
    HTTPS is used, if SSLBump-like HTTPS proxies are in place, they will
    impersonate keyserver.ubuntu.com and generate a certificate with
    keyserver.ubuntu.com in the CN field or in SubjAltName fields of a
    certificate. If such proxy behavior is expected it is necessary to add the
    CA certificate chain containing the intermediate CA of the SSLBump proxy to
    every machine that this code runs on via ca-certs cloud-init directive (via
    cloudinit-userdata model-config) or via other means (such as through a
    custom charm option). Also note that DNS resolution for the hostname in a
    URL is done at a proxy server - not at the client side.
    8-digit (32 bit) key ID
    https://keyserver.ubuntu.com/pks/lookup?search=0x4652B4E6
    16-digit (64 bit) key ID
    https://keyserver.ubuntu.com/pks/lookup?search=0x6E85A86E4652B4E6
    40-digit key ID:
    https://keyserver.ubuntu.com/pks/lookup?search=0x35F77D63B5CEC106C577ED856E85A86E4652B4E6
    :param keyid: An 8, 16 or 40 hex digit keyid to find a key for
    :type keyid: (bytes, str)
    :returns: A key material for the specified GPG key id
    :rtype: (str, bytes)
    :raises: subprocess.CalledProcessError
    """
    # options=mr - machine-readable output (disables html wrappers)
    keyserver_url = ('https://keyserver.ubuntu.com'
                     '/pks/lookup?op=get&options=mr&exact=on&search=0x{}')
    curl_cmd = ['curl', keyserver_url.format(keyid)]
    # use proxy server settings in order to retrieve the key
    return subprocess.check_output(curl_cmd,
                                   env=env_proxy_settings(['https']))


def _dearmor_gpg_key(key_asc):
    """Converts a GPG key in the ASCII armor format to the binary format.
    :param key_asc: A GPG key in ASCII armor format.
    :type key_asc: (str, bytes)
    :returns: A GPG key in binary format
    :rtype: (str, bytes)
    :raises: GPGKeyError
    """
    ps = subprocess.Popen(['gpg', '--dearmor'],
                          stdout=subprocess.PIPE,
                          stderr=subprocess.PIPE,
                          stdin=subprocess.PIPE)
    out, err = ps.communicate(input=key_asc)
    # no need to decode output as it is binary (invalid utf-8), only error
    err = err.decode('utf-8')
    if 'gpg: no valid OpenPGP data found.' in err:
        raise GPGKeyError('Invalid GPG key material. Check your network setup'
                          ' (MTU, routing, DNS) and/or proxy server settings'
                          ' as well as destination keyserver status.')
    else:
        return out


def _write_apt_gpg_keyfile(key_name, key_material):
    """Writes GPG key material into a file at a provided path.
    :param key_name: A key name to use for a key file (could be a fingerprint)
    :type key_name: str
    :param key_material: A GPG key material (binary)
    :type key_material: (str, bytes)
    """
    with open('/etc/apt/trusted.gpg.d/{}.gpg'.format(key_name),
              'wb') as keyf:
        keyf.write(key_material)
