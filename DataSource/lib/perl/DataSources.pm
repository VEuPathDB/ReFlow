package ReFlow::DataSource::DataSources;

use strict;

use ApiCommonWorkflow::Main::DataSource;

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

sub getDataSource {
    my ($self, $dataSourceName) = @_;

    die "can't find resource '$dataSourceName' in xml file $self->{resourcesXmlFile}"
      unless $self->{data}->{resource}->{$dataSourceName};

    return ApiCommonWorkflow::Main::DataSource->new($dataSourceName, $self->{data}->{resource}->{$dataSourceName}, $self);
}

sub _parseXmlFile {
  my ($self, $resourcesXmlFile, $properties) = @_;

  my $xmlString = $self->_substituteMacros($resourcesXmlFile, $properties);
  my $xml = new XML::Simple();
  $self->{data} = eval{ $xml->XMLin($xmlString, SuppressEmpty => undef, KeyAttr => 'resource', ForceArray=>['publication','unpack', 'getAndUnpackOutput']) };
#  print STDERR Dumper $self->{data};
  die "$@\n$xmlString\n" if($@);
}

sub _substituteMacros {
  my ($self, $xmlFile, $props) = @_;

  my $xmlString;
  open(FILE, $xmlFile) || die "Cannot open resources XML file '$xmlFile'\n";
  while (<FILE>) {
    my $line = $_;
    my @macroKeys = /\@\@([\w.]+)\@\@/g;   # allow keys of the form nrdb.release
    foreach my $macroKey (@macroKeys) {
      my $val = $props->getProp($macroKey);
      die "Invalid macro '\@\@$macroKey\@\@' in xml file $xmlFile" unless defined $val;
      $line =~ s/\@\@$macroKey\@\@/$val/g;
    }
    $xmlString .= $line;
  }
  return $xmlString;
}

1;
