<?php
/**
 * Created by PhpStorm.
 * User: billowton
 * Date: 2019/4/29
 * Time: 上午11:13
 */

namespace Es;


/**
 * Class AbstractEsRes
 * @package XiangjuService\Es
 * ES搜索 返回结果
 */
class AbstractEsRes
{


    /**
     * @var $source array 源数据
     */
    private $source;


    /**
     * @var int
     */
    private $total = 0;

    protected $resList = [];
    protected $aggregations = [];



    public function setRes($source,$useGroup=false)
    {
        $this->source = $source;

        if(isset($source['hits']['total'])){
            $this->total = $source['hits']['total'];
        }

        if(isset($source['hits']['hits'])){
            $this->resList = $source['hits']['hits'];
        }

        if(isset($source['aggregations'])){
            $this->aggregations['list'] = $source['aggregations']['res_list']['buckets'];
            $this->aggregations['count'] = $source['aggregations']['res_count']['value'];
            if($useGroup){
                $this->total = $this->aggregations['count'];
                $this->resList = $this->aggregations['list'];
            }
        }
        return $this;
    }


    public function getResList()
    {
        return $this->resList;
    }

    public function getAggregations()
    {
        return $this->aggregations;
    }

    public function getTotal()
    {
        return $this->total;
    }


    public function getSourceData()
    {
        return $this->source;
    }


    public function getTotalFound()
    {
        return $this->total;
    }

}