package ReFlow::Dataset::Datasets;

use strict;

use ReFlow::Dataset::Dataset;

use XML::Simple;
use Data::Dumper;

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
    my ($className) = @_;
    if (!$self->{className2datasets}) {
    }
    return $self->{className2datasets}->{$className};
}

sub _parseXmlFile {
  my ($self, $datasetsFile) = @_;

  my $xml = new XML::Simple();
  $self->{data} = eval{ $xml->XMLin($datasetsFile, SuppressEmpty => undef, KeyAttr => 'resource', ForceArray=>['publication','unpack', 'getAndUnpackOutput', 'resource', 'wdkReference']) };
#  print STDERR Dumper $self->{data};
  die "$@\n$xmlString\nerror processing XML file $resourcesXmlFile\n" if($@);
}

1;
