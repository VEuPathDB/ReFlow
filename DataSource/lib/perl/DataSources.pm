package ReFlow::DataSource::DataSources;

use strict;

use ReFlow::DataSource::DataSource;

use XML::Simple;
use Data::Dumper;

# this is a separate .pm file because it is of general utility

sub new {
  my ($class, $resourcesXmlFile, $properties) = @_;

  my $self = {};

  bless($self,$class);

  $self->{resourcesXmlFile} = "$ENV{GUS_HOME}/lib/xml/datasources/$resourcesXmlFile";

  $self->_parseXmlFile($self->{resourcesXmlFile}, $properties);

  return $self;
}

sub getXmlFile {
  my ($self) = @_;
  return $self->{resourcesXmlFile};
}

sub getDataSourceNames {
  my ($self) = @_;

  my $resources = $self->{data}->{datasetLoader};

  return keys(%$resources);
}

sub getDataSource {
    my ($self, $dataSourceName) = @_;

    die "can't find datasetLoader '$dataSourceName' in xml file $self->{resourcesXmlFile}"
      unless $self->{data}->{datasetLoader}->{$dataSourceName};

    return ReFlow::DataSource::DataSource->new($dataSourceName, $self->{data}->{datasetLoader}->{$dataSourceName}, $self);
}

# static method to publicize elements that need forceArray.  this is used
# by Dataset/Classes.pm in its code generating of resource files
sub getForceArray {
  return ['unpack', 'getAndUnpackOutput', 'datasetLoader'];
}

sub _parseXmlFile {
  my ($self, $resourcesXmlFile, $properties) = @_;

  my $xmlString = $self->_substituteMacros($resourcesXmlFile, $properties);
  my $xml = new XML::Simple();
  my $forceArray = getForceArray();
  $self->{data} = eval{ $xml->XMLin($xmlString, SuppressEmpty => undef, KeyAttr => 'datasetLoader', ForceArray=>$forceArray) };
#  print STDERR Dumper $self->{data};
  die "$@\n$xmlString\nerror processing XML file $resourcesXmlFile\n" if($@);
}

sub _substituteMacros {
  my ($self, $xmlFile, $props) = @_;

  my $xmlString;
  open(FILE, $xmlFile) || die "Cannot open datasetLoader XML file '$xmlFile'\n";
  while (<FILE>) {
    my $line = $_;
    my @macroKeys = /\@\@([\w.]+)\@\@/g;   # allow keys of the form nrdb.release
    foreach my $macroKey (@macroKeys) {
      my $val = $props->getPropRelaxed($macroKey);
      die "Invalid macro '\@\@$macroKey\@\@' in xml file $xmlFile" unless defined $val;
      $line =~ s/\@\@$macroKey\@\@/$val/g;
    }
    $xmlString .= $line;
  }
  return $xmlString;
}


1;
