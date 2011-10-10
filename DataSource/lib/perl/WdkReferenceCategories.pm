package ReFlow::DataSource::WdkReferenceCategories;

use strict;

use XML::Simple;

sub new {
  my ($class, $xmlFile) = @_;

  my $self = {xml_file => $xmlFile};

  bless($self,$class);

  $self->setParsedXml();
  $self->setData();

  return $self;
}

sub getXmlFile {
  my ($self) = @_;

  return $self->{xml_file};
}


sub setParsedXml {
  my ($self) = @_;

  my $xmlFile = $self->getXmlFile();

  my $xmlString = `cat $xmlFile`;
  my $xml = new XML::Simple();
  $self->{parsed_xml} = eval{ $xml->XMLin($xmlString, SuppressEmpty => undef, KeyAttr => {wdkReferenceCategory => "wdkReference"}, ForceArray => 1) };
  die "$@\n$xmlString\nerror processing XML file $xmlFile\n" if($@);

  print Dumper $self->{parsed_xml};
}

sub getParsedXml {
  my ($self) = @_;

  return $self->{parsed_xml};
}

sub setData {
  my ($self) = @_;

  my $parsedXml = $self->getParsedXml();

  my %data;

  foreach my $refCat (@{$parsedXml->{wdkReferenceCategory}}) {
    my $category = $refCat->{category};
    my $references = $refCat->{wdkReference};

    $data{$category} = $references;
  }
  $self->{data} = \%data;
}

sub getData {
  my ($self) = @_;

  return $self->{data};
}

sub getWdkReferencesByCategoryName {
  my ($self, $category) = @_;

  my $data = $self->getData();

  return $data->{$category} || [];
}



1;
