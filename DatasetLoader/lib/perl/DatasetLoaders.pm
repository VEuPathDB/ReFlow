package ReFlow::DatasetLoader::DatasetLoaders;

use strict;

use ReFlow::DatasetLoader::DatasetLoader;

use XML::Simple;
use Data::Dumper;

# this is a separate .pm file because it is of general utility

sub new {
  my ($class, $datasetLoadersXmlFile, $properties) = @_;

  my $self = {};

  bless($self,$class);

  $self->{datasetLoadersXmlFile} = "$ENV{GUS_HOME}/lib/xml/datasetLoaders/$datasetLoadersXmlFile";

  $self->_parseXmlFile($self->{datasetLoadersXmlFile}, $properties);

  return $self;
}

sub getXmlFile {
  my ($self) = @_;
  return $self->{datasetLoadersXmlFile};
}

sub getDatasetNames {
  my ($self) = @_;

  my $resources = $self->{data}->{datasetLoader};

  return keys(%$resources);
}

sub getDatasetLoader {
    my ($self, $datasetName) = @_;

    die "can't find datasetLoader '$datasetName' in xml file $self->{datasetLoadersXmlFile}"
      unless $self->{data}->{datasetLoader}->{$datasetName};

    return ReFlow::DatasetLoader::DatasetLoader->new($datasetName, $self->{data}->{datasetLoader}->{$datasetName}, $self);
}

# static method to publicize elements that need forceArray.  this is used
# by Dataset/Classes.pm in its code generating of resource files
sub getForceArray {
  return ['unpack', 'getAndUnpackOutput', 'datasetLoader'];
}

sub _parseXmlFile {
  my ($self, $datasetLoadersXmlFile, $properties) = @_;

  my $xmlString = $self->_substituteMacros($datasetLoadersXmlFile, $properties);
  my $xml = new XML::Simple();
  my $forceArray = getForceArray();
  $self->{data} = eval{ $xml->XMLin($xmlString, SuppressEmpty => undef, KeyAttr => 'datasetName', ForceArray=>$forceArray) };
#  print STDERR Dumper $self->{data};
  die "$@\n$xmlString\nerror processing XML file $datasetLoadersXmlFile\n" if($@);
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
