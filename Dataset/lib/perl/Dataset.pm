package ReFlow::Dataset::Dataset;

use strict;

use XML::Simple;
use Data::Dumper;

# this is a separate .pm file because it is of general utility

sub new {
  my ($datasetsFile) = @_;

  my $self = {};

  bless($self,$class);

  $self->_parseXmlFile($self->{datasetsFile});

  return $self;
}

sub getClassNamesUsed {

}

sub getDatasetsByClass {

}

sub getProps {
}

sub _parseXmlFile {
  my ($self, $datasetsFile) = @_;

  my $xml = new XML::Simple();
  $self->{data} = eval{ $xml->XMLin($datasetsFile, SuppressEmpty => undef, KeyAttr => 'resource', ForceArray=>['publication','unpack', 'getAndUnpackOutput', 'resource', 'wdkReference']) };
#  print STDERR Dumper $self->{data};
  die "$@\n$xmlString\nerror processing XML file $resourcesXmlFile\n" if($@);
}

1;
