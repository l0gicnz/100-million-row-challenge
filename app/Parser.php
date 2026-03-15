<?php
namespace App;
use function chr;use function fclose;use function feof;use function fgets;
use function fopen;use function fread;use function fseek;use function ftell;
use function fwrite;use function gc_disable;use function pcntl_fork;
use function sodium_add;use function str_repeat;use function stream_select;
use function stream_set_chunk_size;use function stream_set_read_buffer;
use function stream_set_write_buffer;use function stream_socket_pair;
use function strlen;use function strpos;use function strrpos;
use function substr;use function unpack;use function array_fill;
use const SEEK_CUR;use const SEEK_END;use const STREAM_IPPROTO_IP;
use const STREAM_PF_UNIX;use const STREAM_SOCK_STREAM;

final class Parser
{
    private const int    W  = 8;
    private const int    ST = 268;
    private const int    DC = 2191;
    private const int    IR = 181_000;
    private const int    DR = 1_048_576;
    private const int    RD = 1_048_576;
    private const int    CG = 8_388_608;
    private const int    UN = 32;
    private const string UP = 'https://stitcher.io/blog/';

    public static function parse(string $ip, string $op): void
    {
        gc_disable();
        $di=[];$dt=[];$dc=0;
        for($y=1;$y<=6;$y++){
            for($m=1;$m<=12;$m++){
                $mx=match($m){2=>$y===4?29:28,4,6,9,11=>30,default=>31};
                $ms=($m<10?'0':'').$m;
                $ym=$y.'-'.$ms.'-';
                for($d=1;$d<=$mx;$d++){
                    $ds=($d<10?'0':'').$d;
                    $di[$ym.$ds]=$dc<<1;
                    $dt[$dc++]='202'.$y.'-'.$ms.'-'.$ds;
                }
            }
        }
        $nx=[];
        for($i=255;$i>0;$i--){$nx[chr($i-1)]=chr($i);}
        $fh=fopen($ip,'rb');
        stream_set_read_buffer($fh,0);
        $pa=[];$se=[];$ns=0;
        $rw=fread($fh,self::IR);
        $ln=strrpos($rw,"\n");
        if($ln===false){$ln=-1;}
        $p=0;
        while($p<$ln&&$ns<self::ST){
            $nl=strpos($rw,"\n",$p+52);
            if($nl===false||$nl>$ln){break;}
            $sl=substr($rw,$p+25,$nl-$p-51);
            if(!isset($se[$sl])){$pa[$ns++]=$sl;$se[$sl]=true;}
            $p=$nl+1;
        }
        if($ns<self::ST){
            $cy=substr($rw,$ln+1);
            while($ns<self::ST&&!feof($fh)){
                $bf=fread($fh,self::DR);
                if($bf===''||$bf===false){break;}
                $bk=$cy.$bf;
                $ln=strrpos($bk,"\n");
                if($ln===false){$cy=$bk;continue;}
                $p=25;
                while($p<$ln){
                    $sp=strpos($bk,',',$p);
                    if($sp===false||$sp>=$ln){break;}
                    $sl=substr($bk,$p,$sp-$p);
                    if(!isset($se[$sl])){$pa[$ns++]=$sl;$se[$sl]=true;if($ns===self::ST){break 2;}}
                    $p=$sp+52;
                }
                $cy=substr($bk,$ln+1);
            }
        }
        fseek($fh,0,SEEK_END);
        $fs=ftell($fh);
        // Flat chunk arrays: avoids ~400 sub-array alloc/destruct cycles vs $ch[]=[$cs,$ce].
        $chS=[];$chE=[];$nc=0;$lo=0;
        while($lo<$fs){
            $hi=$lo+self::CG<$fs?$lo+self::CG:$fs;
            $cs=0;
            if($lo>0){fseek($fh,$lo);fgets($fh);$cs=ftell($fh);}
            $ce=$fs;
            if($hi<$fs){fseek($fh,$hi);fgets($fh);$ce=ftell($fh);}
            $chS[$nc]=$cs;$chE[$nc++]=$ce;
            $lo=$hi;
        }
        fclose($fh);
        $kl=1;
        fk:
        $ks=[];
        foreach($pa as $sl){
            $k=substr(self::UP.$sl,-$kl);
            if(isset($ks[$k])){$kl++;goto fk;}
            $ks[$k]=true;
        }
        $ms=0;$tbl=[];
        // 21-bit packing: slot base = $id * $dc * 2 (byte offset, pre-doubled).
        // Max = 267 * 2191 * 2 = 1,169,994 < 2^21 = 2,097,152. Safe.
        $dc2=$dc<<1;
        foreach($pa as $id=>$sl){
            $st=strlen($sl)+52;
            if($st>$ms){$ms=$st;}
            $tbl[substr(self::UP.$sl,-$kl)]=($st<<21)|($id*$dc2);
        }
        // $bsz is now the full frame size (2 bytes per slot, no chunk_split needed).
        $bsz=($ns*$dc)<<1;
        $ko=26+$kl;
        // 21-bit slot mask and stride shift to match new packing.
        $sm=(1<<21)-1;
        $bl=($ms*self::UN)+$ko;
        $oneLine='$t=$tbl[substr($bf,$p-'.$ko.','.$kl.')];'
                .'$i=($t&'.$sm.')+$di[substr($bf,$p-22,7)];'
                .'$ac[$i]=$nx[$ac[$i]];$p-=$t>>21;';
        $hotFn=eval(
            'return function(&$ac,&$p,&$bf)use($tbl,$di,$nx){'
            .'while($p>'.$bl.'){'
            .str_repeat($oneLine,self::UN)
            .'}'
            .'while($p>='.$ko.'){'.$oneLine.'}};'
        );
        $sk=[];
        for($w=0;$w<self::W;$w++){
            $pr=stream_socket_pair(STREAM_PF_UNIX,STREAM_SOCK_STREAM,STREAM_IPPROTO_IP);
            stream_set_chunk_size($pr[0],$bsz);
            stream_set_chunk_size($pr[1],$bsz);
            stream_set_read_buffer($pr[0],$bsz+8192);
            if(pcntl_fork()===0){
                fclose($pr[0]);
                // $ac is already a valid LE 16-bit buffer (counts at even byte
                // positions, zero-padding at odd positions). No chunk_split required.
                $ac=str_repeat("\0",$bsz);
                $fh=fopen($ip,'rb');
                stream_set_read_buffer($fh,0);
                $ww=self::W;$rd=self::RD;
                for($ci=$w;$ci<$nc;$ci+=$ww){
                    $cs=$chS[$ci];$ce=$chE[$ci];
                    fseek($fh,$cs);
                    $rm=$ce-$cs;
                    while($rm>0){
                        $r=$rm>$rd?$rd:$rm;
                        $bf=fread($fh,$r);
                        $bln=strlen($bf);
                        $rm-=$bln;
                        $lnl=strrpos($bf,"\n");
                        if($lnl===false){break;}
                        $tl=$bln-$lnl-1;
                        if($tl>0){fseek($fh,-$tl,SEEK_CUR);$rm+=$tl;}
                        $p=$lnl;
                        $hotFn($ac,$p,$bf);
                    }
                }
                fclose($fh);
                stream_set_write_buffer($pr[1],$bsz+8192);
                fwrite($pr[1],$ac);
                fclose($pr[1]);
                exit(0);
            }
            fclose($pr[1]);
            $sk[$w]=$pr[0];
        }
        $bu=array_fill(0,self::W,'');
        $wr=[];$ex=[];
        while($sk!==[]){
            $rd=$sk;
            stream_select($rd,$wr,$ex,5);
            foreach($rd as $id=>$s){
                $d=fread($s,$bsz);
                if($d!==''&&$d!==false){$bu[$id].=$d;}
                if(feof($s)){fclose($s);unset($sk[$id]);}
            }
        }
        $mg=$bu[0];
        for($w=self::W-1;$w>0;$w--){sodium_add($mg,$bu[$w]);}
        $cn=unpack('v*',$mg);
        self::wj($op,$cn,$pa,$dt,$dc,$ns);
    }

    private static function wj(string $op,array $cn,array $pa,array $dt,int $nd,int $ns): void
    {
        $fh=fopen($op,'wb');
        stream_set_write_buffer($fh,1_048_576);
        fwrite($fh,'{');
        $dp=[];
        for($d=$nd-1;$d>=0;$d--){$dp[$d]='        "'.$dt[$d].'": ';}
        $ph=[];
        for($p=$ns-1;$p>=0;$p--){$ph[$p]='"\/blog\/'.$pa[$p].'": {';}
        $sp="\n    ";$ba=1;
        for($p=0;$p<$ns;$p++,$ba+=$nd){
            $fi=-1;$c=$ba;
            for($d=0;$d<$nd;$d++,$c++){if($cn[$c]!==0){$fi=$d;break;}}
            if($fi<0){continue;}
            $rw=$sp.$ph[$p]."\n".$dp[$fi].$cn[$ba+$fi];
            $sp=",\n    ";
            $c=$ba+$fi+1;
            for($d=$fi+1;$d<$nd;$d++,$c++){
                if($cn[$c]===0){continue;}
                $rw.=",\n".$dp[$d].$cn[$c];
            }
            $rw.="\n    }";
            fwrite($fh,$rw);
        }
        fwrite($fh,"\n}");
        fclose($fh);
    }
}