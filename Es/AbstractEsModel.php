<?php
/**
 * Created by PhpStorm.
 * User: billowton
 * Date: 2019/4/28
 * Time: 下午2:16
 */

namespace Es;

use Elasticsearch\Client;
use Elasticsearch\ClientBuilder;
use Elasticsearch\Common\Exceptions\Missing404Exception;
use XiangjuService\Es\AbstractEsRes;


/**
 * Class AbstractEsModel
 * @package XiangjuService\Es
 * ES Model 抽象类
 * 对 elasticsearch 进行了简单的封装。
 */
abstract class AbstractEsModel
{
    /**
     * @var $client Client;
     */
    protected $client;
    protected $_index;
    protected $_type = "document";

    /**
     * @var $searchMapping array 搜索 fieldMapping
     */
    protected $searchMapping = [];
    protected $aggsMapping = [];
    protected $sortModeArr = [];
    protected $page = 1;
    protected $pageSize = 20;
    protected $groupBy = [];
    protected $highlight = [];
    /**
     * @var AbstractEsRes class
     */
    protected $esResClass;

    public function __construct($esResClass = null)
    {
        ///可以选择从配置文件读取
        $hosts = [
            '192.168.1.1:9200',         // IP + Port
            '192.168.1.2',              // Just IP
            'mydomain.server.com:9201', // Domain + Port
            'mydomain2.server.com',     // Just Domain
            'https://localhost',        // SSL to localhost
            'https://192.168.1.3:9200'  // SSL to IP + Port
        ];

        $this->client = ClientBuilder::create()->setHosts($hosts)->build();
        $this->esResClass = $esResClass;
    }


    abstract public function createOrUpdateIndex($id);

    protected function doCreateOrUpdate($id, $data)
    {
        $params = [
            'index' => $this->_index,
            'id' => $id,
            'type' => $this->_type,

        ];

        $paramsBody = $params + [
                'body' => $data
            ];


        try {
            $this->client->get($params);
            $hasExist = true;
        } catch (Missing404Exception $exception) {
            $hasExist = false; //表示不存在
        }
        try {
            if ($hasExist) {
                //更新
                $paramsBody['body'] = ['doc' => $data];
                $paramsBody['retry_on_conflict'] = 5;
                $this->client->update($paramsBody);
            } else {

                $this->client->create($paramsBody);
            }
            return true;
        } catch (\Exception $e) {
            //echo json_encode($paramsBody);exit;
            echo $e->getMessage();
            exit;
            // return false;
        }
    }

    /**
     * @param $fieldName
     * @param $type //聚合 类型  如  sum   avg
     * @param $returnFileName
     * @param $ext array 额外数据
     */
    public function setAggsCommon($fieldName, $type, $returnFieldName = '', $typeExtData = [], $subAggs = [])
    {
        if (empty($returnFieldName)) {
            $returnFieldName = $fieldName;
        }
        $this->aggsMapping[] = [
            'field' => $fieldName,
            'return_field' => $returnFieldName,
            'type' => $type,
            'sub_aggs' => $subAggs,
            'type_ext_data' => $typeExtData,
        ];
        return $this;
    }


    /**
     * @param $filedName
     * @param $value
     * @param bool $exclude
     * @param bool $useLikeMode 类似mysql的 like 模式
     * @return $this|AbstractEsModel
     * 基本查询
     */
    public function setFilter($filedName, $value, $exclude = false, $useLikeMode = false)
    {
        /*  if (in_array($value,[null,''])) { //过滤掉无效的查询
              return $this;
          }*/
        if ($value === '' || $value === null) {//过滤掉无效的查询
            return $this;
        }

        if (is_array($value)) {
            return $this->setFilterArray($filedName, $value, $exclude);
        } else {
            if ($useLikeMode) {
                $value = '*' . $value . '*';
                return $this->doFilter($filedName, $value, $exclude, 'wildcard');
            } else {
                return $this->doFilter($filedName, $value, $exclude);
            }

        }


    }

    public function setHighlight($filedStrArr = [])
    {
        $this->highlight = $filedStrArr;
        return $this;
    }

    public function setFilters(array $qrr)
    {
        foreach ($qrr as $k => $v) {
            $this->setFilter($k, $v);
        }
        return $this;
    }


    /**
     * @param $filedName
     * @param $start
     * @param $end
     * @return AbstractEsModel
     * 范围查询
     */
    public function setFilterRange($filedName, $start, $end)
    {

        $data = [];
        if ($start !== null) {
            $data['gte'] = $start;
        }

        if ($end !== null) {
            $data['lte'] = $end;
        }

        return $this->doFilter($filedName, $data, false, 'range');
    }


    /**
     * @param $filedName
     * @param array $arrayVal
     * @param bool $exclude
     * @return AbstractEsModel
     * 值为数组查询
     */
    public function setFilterArray($filedName, array $arrayVal, $exclude = false)
    {
        return $this->doFilter($filedName, $arrayVal, $exclude, 'match_array');
    }


    /**
     * @param $scriptStr
     * @param bool $exclude
     * @return AbstractEsModel
     * 自定义脚本过滤 ，为 inline 模式
     */
    public function setScriptFilter($scriptStr, $exclude = false)
    {
        return $this->doFilter("", $scriptStr, $exclude, 'script');
    }


    /**
     * @param array $fields
     * @param $keyword
     * 多字段查询   适用于 同一个值 作用于多个字段
     */
    public function setMultiFieldsFilter(array $fields, $keyword, $useLikeMode = false)
    {
        if ($useLikeMode === false) {
            return $this->doFilter(null, ['query' => $keyword, 'fields' => $fields], false, 'multi_match');
        } else {
            $valueArr = [];
            foreach ($fields as $field) {
                $valueArr[] = ['wildcard' => [$field => '*' . $keyword . '*']];
            }
            return $this->doFilter(null, $valueArr, false, 'multi_match_like');
        }


    }


    private function doFilter($filedName, $value, $exclude = false, $mode = 'match')
    {
        if ($value === null) {
            return $this;
        }
        if (in_array($filedName, array_column($this->searchMapping, 'filed'))) {
            $this->clearFilterByFieldName($filedName);
        }
        $this->searchMapping[] = [
            'filed' => $filedName,
            'value' => $value,
            'mode' => $mode,
            'exclude' => $exclude,
        ];
        return $this;
    }


    public function clearFilterByFieldName($fieldName)
    {
        foreach ($this->searchMapping as $k => $item) {
            if ($item['filed'] == $fieldName) {
                unset($this->searchMapping[$k]);
            }
        }
        return $this;
    }

    /**
     * @param int $page
     * @param int $pageSize
     * @return $this
     * 分页查询 （ 对 使用了group by的 无效）
     * Tips: 特别注意 最多翻 10000条数据  如果20条一页 只能翻500页
     */
    public function setLimitsByPage($page = 1, $pageSize = 20)
    {
        $this->page = $page;
        $this->pageSize = $pageSize;
        return $this;
    }

    /**
     * @param $esRes
     * @return $this
     * 指定结果集
     */
    public function setEsRes($esRes)
    {
        $this->esResClass = $esRes;
        return $this;
    }


    /**
     * @param $sortFiled
     * @param string $mode
     * @param bool $overwrite
     * @return $this
     * 设置排序 （ 对 使用了group by的 无效）
     */
    public function setSortMode($sortFiled, $mode = "desc", $overwrite = true)
    {
        if ($overwrite) {
            $this->sortModeArr = [$sortFiled => $mode];
        } else {
            $this->sortModeArr[$sortFiled] = $mode;
        }
        return $this;
    }


    /**
     * @param $filed
     * @param int $maxSize
     * @param string $countSort //默认通过 count 排序
     * @param array $subAggs 对分组后 再进行 聚合，比如 AVG  SUM COUNt 等操作 ，自定义实现 参考elasticsearch-php 文档
     *                      比如需要对分组后 求 平均值 可以 ["my_res_show_name"=> ['avg'=>['field'=>'myFieldName']]]
     *                      myFieldName 字段进行avg计算，avg，field 为固定写法,my_res_show_name 为最终返回的字段
     * @return $this
     *  特别说明 ，此方法使用后 没法使用分页，
     *             最多直接返回1000条数据(可以调整，建议1000以内)，
     *             且返回的数据结构格式与其他的不同(结构为 key ,doc_count 其中key为group by 字段)
     */
    public function setGroupBy($filed, $maxSize = 1000, $countSort = 'desc', $subAggs = [])
    {
        $this->groupBy = [
            'filed' => $filed,
            'sort' => $countSort,
            'size' => $maxSize,
            'sub_aggs' => $subAggs,
        ];
        return $this;
    }


    /**
     * @param array $selectFields 指定查询字段 否则是全部
     */
    public function setSelect(array $selectFields = [])
    {
        if (!is_array($selectFields)) {
            $this->selectFields = (array)$selectFields;
        }
    }

    /**
     * 重置 过滤选项
     */
    public function resetFilters()
    {
        $this->searchMapping = [];
        $this->aggsMapping = [];
        $this->sortModeArr = [];
        $this->page = 1;
        $this->pageSize = 20;
        $this->groupBy = [];
        $this->selectFields = [];
    }

    /**
     * 重置 aggsMapping
     */
    public function resetAggs()
    {
        $this->aggsMapping = [];

    }


    /**
     * 重置 groupBy
     */
    public function resetGroupBy()
    {
        $this->groupBy = [];

    }


    /**
     * @return AbstractEsRes
     */
    public function query()
    {

        $params = $this->parseQuery();


        $result = $this->nativeQuery($params);


        if (empty($this->esResClass)) {
            $this->esResClass = new AbstractEsRes();
        }


        $useGroup = false;
        if (!empty($this->groupBy)) {
            $useGroup = true;
        }

        /**
         * @var  $tmpRes AbstractEsRes;
         */
        $tmpRes = new $this->esResClass;
        return $tmpRes->setRes($result, $useGroup);


    }

    public function parseQuery()
    {
        $params = [
            'size' => $this->pageSize,
            'from' => ($this->page - 1) * $this->pageSize,
            //'body' =>$queryBody
        ];

        $qm = 'query';
        $queryBody = [];
        foreach ($this->searchMapping as $item) {
            //$item['value'];
            // $item['mode'];

            switch ($item['mode']) {
                case 'range':
                case 'wildcard':
                case 'match':
                    $filed = $item['filed'];
                    if ($item['exclude'] === true) {
                        $sF = 'must_not';
                    } else {
                        $sF = 'must';
                    }
                    $queryBody[$qm]['bool'][$sF][] = [
                        $item['mode'] => [$filed => $item['value']]
                    ];
                    break;


                    break;
                case 'multi_match':
                    $queryBody[$qm]['bool']['must'][] = [
                        'multi_match' => $item['value']
                    ];
                    break;
                case 'multi_match_like':
//
                    if ($item['exclude'] === true) {
                        $sF = 'must_not';
                    } else {
                        $sF = 'must';
                    }
                    $tmpQ = [];
                    foreach ($item['value'] as $value) {
                        $tmpQ['bool']['should'][] = $value;
                    }
                    $queryBody[$qm]['bool'][$sF][] = $tmpQ;

                    break;
                case 'match_array':
                    if ($item['exclude'] === true) {
                        $sF = 'must_not';
                    } else {
                        $sF = 'must';
                    }
                    $filed = $item['filed'];
                    $tmpQ = [];

                    foreach ($item['value'] as $value) {
                        $tmpQ['bool']['should'][] = [
                            "match" => [
                                $filed => $value
                            ]
                        ];
                    }

                    $queryBody[$qm]['bool'][$sF][] = $tmpQ;

                    break;
                case 'script':
                    if ($item['exclude'] === true) {
                        $sF = 'must_not';
                    } else {
                        $sF = 'must';
                    }
                    $scriptQ = [
                        'script' => [
                            'script' => [
                                'inline' => $item['value'],
                                'lang' => 'painless'
                            ]
                        ]
                    ];
                    $queryBody[$qm]['bool'][$sF][] = $scriptQ;

                    break;
            }

        }


        if (!empty($this->sortModeArr)) {
            foreach ($this->sortModeArr as $key => $sortMode) {
                $params['sort'] = [$key . ":" . $sortMode];
            }
        }

        if (!empty($this->selectFields)) {
            $params['_source'] = $this->selectFields;
        }

        if (!empty($this->groupBy)) {  //分组则不支持分页，只能直接返回1000条数据

            $tmpF = $this->groupBy['filed'];
            $tmpSize = isset($this->groupBy['size']) ? $this->groupBy['size'] : 1000;
            $tmpFSort = $this->groupBy['sort'];


            $queryBody['aggs'] = [
                "res_list" => [
                    "terms" => [
                        "field" => $tmpF,
                        "size" => $tmpSize
                    ]
                ],
                "res_count" => [  //数量统计
                    "cardinality" => [
                        "field" => $tmpF
                    ]
                ]
            ];
            if (!empty($tmpFSort)) {
                $queryBody['aggs']["res_list"]["terms"]["order"] = ["_count" => $tmpFSort];
            }
            if (!empty($this->groupBy['sub_aggs'])) {
                $queryBody['aggs']["res_list"]["aggs"] = $this->groupBy['sub_aggs'];//对group by 后再处理 ，可以自定义
            }
            $params['size'] = 0;
        }

        $params['body'] = $queryBody;


        if (!empty($this->highlight)) {
            //$params['highlight']['fields'];
            foreach ($this->highlight as $filedItem) {
                $params['body']['highlight']['fields'][$filedItem] = new \stdClass();
            }
        }

        if (!empty($this->aggsMapping)) {
            foreach ($this->aggsMapping as $aggItem) {
                $tmpD = [
                    $aggItem['type'] => [
                        'field' => $aggItem['field']
                    ]
                ];

                if (!empty($aggItem['type_ext_data'])) {

                    $tmpD[$aggItem['type']] += $aggItem['type_ext_data'];
                }
                if (!empty($aggItem['sub_aggs'])) {
                    $tmpD["aggs"] = $aggItem['sub_aggs'];// 后再处理 ，可以自定义
                }

                $params['body']['aggs'][$aggItem['return_field']] = $tmpD;
            }
        }

        //  echo json_encode($params)."\n\n";
        return $params;
    }

    public function deleteById($id)
    {
        if (empty($params['index'])) {
            $params['index'] = $this->_index;
        }
        if (empty($params['type'])) {
            $params['type'] = $this->_type;
        }

        $params['id'] = $id;


        try {
            $this->client->delete($params);
            return true;
        } catch (\Exception $e) {
            //echo $e->getMessage();
            // exit;
            return false;
            //  return [];
        }

    }


    /**
     * @param $params
     * @return array
     * 如果有其他需求 可以直接调用 方法，以便实现自定义查询
     */
    public function nativeQuery($params)
    {
        if (empty($params['index'])) {
            $params['index'] = $this->_index;
        }
        if (empty($params['type'])) {
            $params['type'] = $this->_type;
        }


        try {
            return $this->client->search($params);
        } catch (\Exception $e) {
            echo $e->getMessage();
            exit;

            //  return [];
        }
    }

    public function deleteByQuery()
    {
        $params = $this->parseQuery();
        if (empty($params['index'])) {
            $params['index'] = $this->_index;
        }
        if (empty($params['type'])) {
            $params['type'] = $this->_type;
        }
        try {
            return $this->client->deleteByQuery($params);
        } catch (\Exception $e) {
            echo $e->getMessage();
            exit;
        }

    }


    public function deleteByRes(AbstractEsRes $res)
    {
        //删除
        $ids = array_column($res->getResList(), '_id');
        if (!empty($ids)) {
            foreach ($ids as $id) {
                $this->deleteById($id);
            }
        }

    }
}