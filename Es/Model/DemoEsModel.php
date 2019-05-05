<?php
/**
 * Created by PhpStorm.
 * User: xiangju
 * Date: 2019/4/28
 * Time: 下午2:18
 */

namespace Es\Model;



use Es\AbstractEsModel;
use Es\Res\DemoEsRes;


/**
 * Class CustomerEsModel
 * @package XiangjuService\Es\Model
 * ES 模型
 * 通过此类对 AbstractEsModel 覆盖 $_index ，实际使用 使用拓展类
 */
class DemoEsModel extends AbstractEsModel
{
    protected $_index = 'demo';


    /**
     * 需要 索引的字段
     */
    const ATTR_FILED1   = "field1";
    const ATTR_FILED2      = "field2";


    public function __construct()
    {

        //可以在初始化时就指定定 结果集，可以通过 $this->setEsRes() 设置
        $esRes = DemoEsRes::class;
        parent::__construct($esRes);
    }


    /**
     * @param $id
     * Demo function
     * 比如可以这样 通过主键更新 es 索引数据
     */
    public function createOrUpdateIndex($id)
    {

        $data = [
          self::ATTR_FILED1=>'11',
          self::ATTR_FILED2=>'22',
        ];

       $res = $this->doCreateOrUpdate($id, $data);

        return $res;

    }




}
