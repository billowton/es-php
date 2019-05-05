<?php
/**
 * Created by PhpStorm.
 * User: billowton
 * Date: 2019/5/5
 * Time: 上午9:31
 */


require_once "./vendor/autoload.php";

use Es\Model\DemoEsModel;

//简单使用

$dm = new DemoEsModel();

/**
 * @var $res \Es\Res\DemoEsRes;
 */
$res = $dm->setEsRes(\Es\Res\DemoEsRes::class)//设置查询结果集
    ->setFilter(DemoEsModel::ATTR_FILED1,"11") //查询DemoEsModel::ATTR_FILED1 为 11的
    ->query();//执行查询


var_dump($res->getResList()); //打印结果