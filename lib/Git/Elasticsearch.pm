package Git::Elasticsearch;

use strict;
use warnings;

use Git;
use App::Prove::Elasticsearch::Utils;

our $index = 'git';
our $scale = 1000;

our $idx;
our $bulk_helper;

sub index_log {
    my ($stop_at_sha) = @_;
    $stop_at_sha //= '';

    my $conf = App::Prove::Elasticsearch::Utils::process_configuration();
    my $port = $conf->{'server.port'} ? ':'.$conf->{'server.port'} : '';
    die "server must be specified" unless $conf->{'server.host'};
    die("port must be specified") unless $port;
    my $serveraddress = "$conf->{'server.host'}$port";
    $e //= Search::Elasticsearch->new(
        nodes           => $serveraddress,
    );

    #Batch in blobs to not OOM
    my @command = (qw{log --all -M --find-copies-harder --numstat}, "-$scale");
    my ($cnt,@skip);

    while( my @log = Git::command((@command,@skip)) ) {
        last unless @log;
        my %parsed = parse_log($stop_at_sha,@log);

		my @records;
        foreach my $sha ( keys(%parsed) ) {
            foreach my $file ( @{$parsed{$sha}{files}} ) {
                print "Index $file->{name} at $sha\n";
                $file->{sha}    = $sha;
                $file->{author} = $parsed{$sha}{author};
                $file->{email}  = $parsed{$sha}{email};
                $file->{date}   = $parsed{$sha}{date};
				push(@records,$file);
            }
        }
		bulk_index($es,@records);

        last if $stop_at_sha && $parsed{$stop_at_sha};
        $cnt++;
        @skip = ('--skip',$cnt*$scale);
    }
    return 1;
}

sub parse_log {
    my ($stop_at_sha,@log) = @_;

    my %parsed;
    my ($sha,$last_sha);
    foreach my $line (@log) {
        if ( my ($sha_parsed) = $line =~ m/^commit ([A-Fa-f0-9]*)$/ ) {
            $sha = $sha_parsed;
            last if $last_sha && $last_sha eq $stop_at_sha;
            $last_sha = $sha;
            $parsed{$sha} = {};
            next;
        }

        if ( my ($author,$email) = $line =~ m/^Author: (.*) <(.*)>$/ ) {
            $parsed{$sha}{author} = $author;
            $parsed{$sha}{email}  = $email;
            next;
        }

        if (my ($date) = $line =~ m/^Date:\s*(.*)$/ ) {
            $parsed{$sha}{date} = $date;
            next;
        }

        if ( my ($add,$del,$file) = $line =~ m/^\s*(\d+)\s*(\d+)\s*(.*)/) {
            $parsed{$sha}{files} //= [];
            push(@{$parsed{$sha}{files}}, {
                name => $file,
                add  => $add,
                del  => $del,
                patch => join("\n", Git::command((qw{format-patch -1 --stdout},$sha))),
            });
            next;
        }

    }
    return %parsed;
}

sub bulk_index {
    my ($es,@results) = @_;
    $bulk_helper //= $e->bulk_helper(
        index    => $index,
        type     => $index,
    );

    $idx //= App::Prove::Elasticsearch::Utils::get_last_index($e,$index);

    $bulk_helper->index(map { $idx++; { id => $idx, source => $_ } } @results);
    $bulk_helper->flush();
	return 1;
}

1;
